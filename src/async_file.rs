use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek},
};

pub trait AsyncFile: AsyncRead + AsyncSeek + Unpin + Send {}

impl<T: AsyncFile> AsyncFile for &mut T {}

impl AsyncFile for File {}
