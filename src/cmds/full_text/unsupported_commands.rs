impl FtUnsupported {
    pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        let command_name = arg(&frame, 0, "ERR empty command")?.to_ascii_uppercase();
        Ok(Self { command_name })
    }

    pub fn apply(self) -> Result<Frame, Error> {
        Ok(Frame::Error(format!(
            "ERR unsupported full-text command {}",
            self.command_name
        )))
    }

    pub async fn apply_async(self) -> Result<Frame, Error> {
        self.apply()
    }
}
