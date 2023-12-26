use std::io::Write;

pub struct SSTable<Writer: Write> {
    writer: Writer,
    data: Vec<(String, String)>,
}

struct PersistFormat {
    key_len: usize,
    value_len: usize,
    key: String,
    value: String,
}

impl<Writer: Write> SSTable<Writer> {
    pub fn new(writer: Writer) -> SSTable<Writer> {
        SSTable {
            writer,
            data: Vec::new(),
        }
    }

    pub fn with_data(writer: Writer, data: Vec<(String, String)>) -> SSTable<Writer> {
        SSTable { writer, data: data }
    }
}

impl<Writer: Write> SSTable<Writer> {
    pub fn add(&mut self, key: String, value: String) {
        self.data.push((key, value))
    }

    pub fn write_table(self: &mut Self) -> Result<(), std::io::Error> {
        let writer = &mut self.writer;

        for (key, value) in &self.data {
            let entry = PersistFormat {
                key_len: key.len(),
                value_len: value.len(),
                key: key.clone(),
                value: value.clone(),
            };

            writer.write(entry.serialize().as_slice())?;
        }

        return Ok(());
    }
}

impl PersistFormat {
    fn serialize(&self) -> Vec<u8> {
        return [
            &self.key_len.to_be_bytes(),
            &self.value_len.to_be_bytes(),
            self.key.as_bytes(),
            self.value.as_bytes(),
        ]
        .concat();
    }
}
