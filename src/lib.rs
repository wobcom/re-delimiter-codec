use regex::bytes::Regex;
use std::cmp;
use std::io::Error;
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

#[derive(Clone)]
pub struct REDelimiterCodec {
    regex: Regex,
    is_discarding: bool,
    next_index: usize,
    max_length: usize,
}

#[derive(Debug)]
pub enum REDelimiterCodecError {
    MaxChunkLengthExceeded,
    Io(Error),
}

impl From<Error> for REDelimiterCodecError {
    fn from(e: Error) -> Self {
        REDelimiterCodecError::Io(e)
    }
}

impl REDelimiterCodec {
    pub fn new(regex: Regex) -> Self {
        REDelimiterCodec {
            regex,
            is_discarding: false,
            next_index: 0,
            max_length: usize::MAX,
        }
    }

    pub fn new_with_max_length(regex: Regex, max_length: usize) -> Self {
        REDelimiterCodec {
            max_length,
            ..REDelimiterCodec::new(regex)
        }
    }
}

impl Decoder for REDelimiterCodec {
    type Item = Bytes;
    type Error = REDelimiterCodecError;

    // implementation details shamelessly stolen from AnyDelimiterCodec
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let read_to = cmp::min(self.max_length, buf.len()); // delimiter size is dynamic
            let slice = &buf[self.next_index..read_to];

            let new_chunk_offset = self.regex.find(slice);

            match (self.is_discarding, new_chunk_offset) {
                (true, Some(re_match)) => {
                    // some delimiter found, but we were discarding
                    // + re_match.len() => chop off with delimiter
                    buf.advance(re_match.start() + self.next_index + re_match.len());
                    self.is_discarding = false;
                    self.next_index = 0; // rewind to start as incriminated section was chopped
                    // no return, continue reading buffer in loop
                }
                (true, None) => {
                    // discarding and we didn't find delimiter
                    // no delimiter found till end of slice
                    buf.advance(read_to); // chop off
                    // we continue discarding (self.is_discarding still true)
                    self.next_index = 0;

                    if buf.is_empty() {
                        return Ok(None); // waiter! more bytes please 😋️
                    }
                }
                (false, Some(re_match)) => {
                    // not discarding and we found some delimiter
                    let new_chunk_index = re_match.start() + self.next_index;
                    self.next_index = 0;
                    // + re_match.len()  => message will contain delimiter
                    let chunk = buf.split_to(new_chunk_index + re_match.len());

                    return Ok(Some(chunk.freeze()));
                }
                // no delimiter found and reached max length
                (false, None) if buf.len() > self.max_length => {
                    // return error (max length reached) and start discarding on next call
                    self.is_discarding = true;

                    return Err(REDelimiterCodecError::MaxChunkLengthExceeded);
                }
                (false, None) => {
                    // no delimiter found but didn't reach length limit
                    self.next_index = read_to; // skip it!

                    return Ok(None); // waiter... I am still hungry 🥺️
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::REDelimiterCodec;
    use std::assert_matches;
    use std::cmp::Ordering;
    use std::io::{Error, ErrorKind};
    use tokio_stream::StreamExt;
    use tokio_test::io::Builder;
    use tokio_util::codec::FramedRead;

    #[tokio::test]
    async fn test_chunks() {
        let messages = Builder::new()
            .read(
                b"\
% Test data comment
% Another comment


ADD 6577",
            )
            .read(
                b"\
6764

object-typ:     yes
garbled:        maybe
mixed-encoding: for sure

ADD 65776765

object-typ:     yes
garbled:        maybe
mixed-encoding: for sure

",
            )
            .build();
        let first_message = b"\
% Test data comment
% Another comment


ADD 65776764

object-typ:     yes
garbled:        maybe
mixed-encoding: for sure

";
        let second_message = b"\
ADD 65776765

object-typ:     yes
garbled:        maybe
mixed-encoding: for sure

";

        let mut reader = FramedRead::new(
            messages,
            REDelimiterCodec::new(Regex::new(r"(?R)\n[^%][^AD][^DE][^DL].*\n\n").unwrap()),
        );

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(first_message), Ordering::Equal);

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(second_message), Ordering::Equal);
    }

    #[tokio::test]
    async fn test_io_error_signalled() {
        let ioe = Builder::new()
            .read(b"aslkjdlk\n\n")
            .read_error(Error::new(ErrorKind::BrokenPipe, "connection closed"))
            .build();

        let mut reader = FramedRead::new(
            ioe,
            REDelimiterCodec::new(Regex::new(r"will_never_match").unwrap()),
        );

        assert_matches!(reader.next().await, Some(Err(REDelimiterCodecError::Io(_))));
    }

    #[tokio::test]
    async fn test_maximum_length_signalled_and_recovers() {
        let message = Builder::new().read(b"dog;nutria;swan;duck;human;").build();
        let message_1 = b"dog;";
        let message_3 = b"swan;";

        let mut reader = FramedRead::new(
            message,
            REDelimiterCodec::new_with_max_length(Regex::new(r";+").unwrap(), 6),
        );

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(message_1), Ordering::Equal);

        assert_matches!(
            reader.next().await,
            Some(Err(REDelimiterCodecError::MaxChunkLengthExceeded))
        );

        assert_matches!(reader.next().await, None);

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(message_3), Ordering::Equal);
    }
}
