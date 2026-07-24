pub enum WasmCommand {
    Load {
        name: String,
        bytes: Vec<u8>,
    },
    Call {
        name: String,
        function: String,
        args: Vec<String>,
        read_only: bool,
        command_name: &'static str,
    },
    Scan {
        name: String,
        function: String,
        prefix: String,
        limit: usize,
    },
    Delete {
        name: String,
    },
    FunctionLoad {
        name: String,
        bytes: Vec<u8>,
    },
    FunctionDelete {
        name: String,
    },
    FunctionList,
    List,
}

impl WasmCommand {
    pub(crate) fn command_name(&self) -> &'static str {
        match self {
            Self::Load { .. } => "WASM.LOAD",
            Self::Call { command_name, .. } => command_name,
            Self::Scan { .. } => "WASM.SCAN",
            Self::Delete { .. } => "WASM.DEL",
            Self::FunctionLoad { .. } | Self::FunctionDelete { .. } | Self::FunctionList => {
                "FUNCTION"
            }
            Self::List => "WASM.LIST",
        }
    }

    pub(crate) fn may_write_data(&self) -> bool {
        match self {
            Self::Call { read_only, .. } => !read_only,
            Self::Load { .. }
            | Self::Delete { .. }
            | Self::FunctionLoad { .. }
            | Self::FunctionDelete { .. } => true,
            Self::Scan { .. } | Self::FunctionList | Self::List => false,
        }
    }
}
