use std::fmt::Write;

pub struct SSTable<Writer: Write> {
    writer: Writer,
    data: Vec<(String, String)>,
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

    fn write_table(self: &mut Self) -> Result<(), std::fmt::Error> {
        let writer = &mut self.writer;

        for (key, value) in &self.data {
            let entry = format!("{key}{value}");
            let entry_size = entry.len();

            let entry = format!("{entry_size}{entry}");
            writer.write_str(&entry)?;
        }

        return Ok(());
    }

    // Couldn't fight borrow checker to call this in write_table D:
    // fn write_entry(&mut self, key: &String, value: &String) -> Result<(), std::fmt::Error> {
    //     let entry = format!("{key}{value}");
    //     let entry_size = entry.len();

    //     let entry = format!("{entry_size}{entry}");
    //     self.writer.write_str(&entry)
    // }
}
