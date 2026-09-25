//! A small JSON reader that keeps numbers as their text, so a number reads back exactly as the
//! type it was written from says.

/// JSON, its numbers kept as their text.
#[derive(Debug)]
pub(super) enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

/// `text`, JSON the engine wrote.
pub(super) fn parse(text: &str) -> Json {
    let mut reader = Reader {
        bytes: text.as_bytes(),
        at: 0,
    };
    let json = reader.value();
    reader.space();
    assert_eq!(reader.at, text.len(), "trailing text after JSON: {text}");
    json
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn space(&mut self) {
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_whitespace) {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) {
        self.space();
        assert_eq!(self.bytes[self.at], byte, "JSON at {}", self.at);
        self.at += 1;
    }

    fn value(&mut self) -> Json {
        self.space();
        match self.bytes[self.at] {
            b'n' => self.word("null", Json::Null),
            b't' => self.word("true", Json::Bool(true)),
            b'f' => self.word("false", Json::Bool(false)),
            b'"' => Json::String(self.string()),
            b'[' => {
                self.at += 1;
                let mut items = Vec::new();
                self.space();
                if self.bytes[self.at] == b']' {
                    self.at += 1;
                    return Json::Array(items);
                }
                loop {
                    items.push(self.value());
                    self.space();
                    self.at += 1;
                    if self.bytes[self.at - 1] == b']' {
                        return Json::Array(items);
                    }
                }
            }
            b'{' => {
                self.at += 1;
                let mut members = Vec::new();
                self.space();
                if self.bytes[self.at] == b'}' {
                    self.at += 1;
                    return Json::Object(members);
                }
                loop {
                    self.space();
                    let name = self.string();
                    self.eat(b':');
                    members.push((name, self.value()));
                    self.space();
                    self.at += 1;
                    if self.bytes[self.at - 1] == b'}' {
                        return Json::Object(members);
                    }
                }
            }
            _ => {
                let start = self.at;
                while self.bytes.get(self.at).is_some_and(|byte| {
                    matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                }) {
                    self.at += 1;
                }
                Json::Number(String::from_utf8(self.bytes[start..self.at].to_vec()).expect("ASCII"))
            }
        }
    }

    fn word(&mut self, word: &str, json: Json) -> Json {
        assert!(
            self.bytes[self.at..].starts_with(word.as_bytes()),
            "JSON at {}",
            self.at
        );
        self.at += word.len();
        json
    }

    fn string(&mut self) -> String {
        let start = self.at;
        self.at += 1;
        while self.bytes[self.at] != b'"' {
            self.at += if self.bytes[self.at] == b'\\' { 2 } else { 1 };
        }
        self.at += 1;
        let quoted = std::str::from_utf8(&self.bytes[start..self.at]).expect("UTF-8");
        serde_json::from_str(quoted).expect("a JSON string")
    }
}
