// Adapted from: https://github.com/pola-rs/polars/blob/main/crates/polars-io/src/csv/read/utils.rs#L10
// and https://github.com/pola-rs/polars/blob/main/crates/polars-io/src/csv/read/parser.rs#L124
// and https://github.com/pola-rs/polars/blob/main/crates/polars-io/src/csv/read/parser.rs#L310
// Which has the following license:
// Copyright (c) 2020 Ritchie Vink
// Some portions Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use oxrdf::Triple;
use crate::{NTriplesParser, TurtleParser, TurtleSyntaxError};
use crate::ntriples::FromSliceNTriplesReader;
use crate::turtle::FromSliceTurtleReader;

const PERIOD_CHAR: u8 = b'.';
const NEWLINE_CHAR: u8 = b'\n';

//Helper class to create reusable code for parallel parsing of either NTriples or Turtle.
#[derive(Clone)]
enum TurtleParserOrNTriplesParser {
    TurtleParser(TurtleParser),
    NTriplesParser(NTriplesParser),
}

impl TurtleParserOrNTriplesParser {
    pub fn parse_slice(self, slice: &[u8]) -> FromSliceTurtleOrNTriplesReader<'_> {
        match self {
            TurtleParserOrNTriplesParser::TurtleParser(tp) => {
                FromSliceTurtleOrNTriplesReader::FromSliceTurtleReader(tp.parse_slice(slice))
            }
            TurtleParserOrNTriplesParser::NTriplesParser(ntp) => {
                FromSliceTurtleOrNTriplesReader::FromSliceNTriplesReader(ntp.parse_slice(slice))
            }
        }
    }
}

enum FromSliceTurtleOrNTriplesReader<'a> {
    FromSliceTurtleReader(FromSliceTurtleReader<'a>),
    FromSliceNTriplesReader(FromSliceNTriplesReader<'a>)
}

impl<'a> Iterator for FromSliceTurtleOrNTriplesReader<'a> {
    type Item = Result<Triple, TurtleSyntaxError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FromSliceTurtleOrNTriplesReader::FromSliceTurtleReader(t) => {
                t.next()
            }
            FromSliceTurtleOrNTriplesReader::FromSliceNTriplesReader(nt) => {
                nt.next()
            }
        }
    }
}

// Given a number of desired chunks, corresponding to threads find offsets that break the file into chunks that can be read in parallel.
// Parser should not be reused, hence it is passed by value.
pub fn get_turtle_file_chunks(bytes: &[u8],
                              n_chunks: usize,
                              parser: TurtleParser) -> Vec<(usize, usize)> {
    let parser = TurtleParserOrNTriplesParser::TurtleParser(parser);
    get_ntriples_or_turtle_file_chunks(bytes, n_chunks, parser)
}

// Given a number of desired chunks, corresponding to threads find offsets that break the file into chunks that can be read in parallel.
// Parser should not be reused, hence it is passed by value.
pub fn get_ntriples_file_chunks(bytes: &[u8],
                              n_chunks: usize,
                              parser: NTriplesParser) -> Vec<(usize, usize)> {
    let parser = TurtleParserOrNTriplesParser::NTriplesParser(parser);
    get_ntriples_or_turtle_file_chunks(bytes, n_chunks, parser)
}

// Helper function that creates the chunks described above for either NTriples or Turtle.
#[allow(clippy::needless_pass_by_value)]
fn get_ntriples_or_turtle_file_chunks(
    bytes: &[u8],
    n_chunks: usize,
    parser: TurtleParserOrNTriplesParser,
) -> Vec<(usize, usize)> {
    let mut last_pos = 0;
    let total_len = bytes.len();
    let chunk_size = total_len / n_chunks;
    let mut offsets = Vec::with_capacity(n_chunks);
    for _ in 0..n_chunks {
        let search_pos = last_pos + chunk_size;

        if search_pos >= bytes.len() {
            break;
        }

        let end_pos = match next_terminating_char(parser.clone(), &bytes[search_pos..]) {
            Some(pos) => search_pos + pos,
            None => {
                // We keep the valid chunks we found, and add (outside the loop) the rest of the bytes as a chunk.
                break;
            }
        };
        offsets.push((last_pos, end_pos));
        last_pos = end_pos;
    }
    if last_pos < total_len {
        offsets.push((last_pos, total_len));
    }
    offsets
}

// Heuristically, we assume that a period is terminating (a triple) if we can start immediately after it and parse three triples.
// Parser should not be reused, hence it is passed by value.
// If no such period can be found, looking at 1000 consecutive periods, we give up.
// Important to keep this number this high, as some TTL files can have a lot of periods.
#[allow(clippy::needless_pass_by_value)]
fn next_terminating_char(parser: TurtleParserOrNTriplesParser, mut input: &[u8]) -> Option<usize> {
    fn accept(parser: TurtleParserOrNTriplesParser, input: &[u8]) -> bool {
        let mut f = parser.parse_slice(input);
        for _ in 0..3 {
            if let Some(r) = f.next() {
                if r.is_err() {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
    let mut total_pos = 0;
    let use_eot_char = match &parser {
        TurtleParserOrNTriplesParser::TurtleParser(_) => {PERIOD_CHAR}
        TurtleParserOrNTriplesParser::NTriplesParser(_) => {NEWLINE_CHAR}
    };
    for _ in 0..1_000 {
        let pos = memchr::memchr(use_eot_char, input)? + 1;
        if input.len() - pos == 0 {
            return None;
        }
        let new_input = &input[pos..];
        let p = parser.clone();
        let accepted = accept(p, new_input);
        if accepted {
            return Some(total_pos + pos);
        }
        input = &input[pos + 1..];
        total_pos += pos + 1;
    }
    None
}
