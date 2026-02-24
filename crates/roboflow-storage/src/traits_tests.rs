// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

#[cfg(test)]
mod tests {
    use crate::error::StorageError;
    use crate::{ObjectMetadata, SeekRead, Storage};
    use std::io::{Read, Write};
    use std::path::Path;

    struct MockStorage;

    impl Storage for MockStorage {
        fn reader(
            &self,
            _path: &Path,
        ) -> crate::error::StorageResult<Box<dyn Read + Send + 'static>> {
            Err(StorageError::NotFound(_path.to_string_lossy().to_string()))
        }

        fn writer(
            &self,
            _path: &Path,
        ) -> crate::error::StorageResult<Box<dyn Write + Send + 'static>> {
            Err(StorageError::Other("mock".to_string()))
        }

        fn exists(&self, _path: &Path) -> bool {
            false
        }

        fn size(&self, path: &Path) -> crate::error::StorageResult<u64> {
            Err(StorageError::NotFound(path.to_string_lossy().to_string()))
        }

        fn metadata(&self, path: &Path) -> crate::error::StorageResult<ObjectMetadata> {
            Err(StorageError::NotFound(path.to_string_lossy().to_string()))
        }

        fn list(&self, _prefix: &Path) -> crate::error::StorageResult<Vec<ObjectMetadata>> {
            Ok(Vec::new())
        }

        fn delete(&self, path: &Path) -> crate::error::StorageResult<()> {
            Err(StorageError::NotFound(path.to_string_lossy().to_string()))
        }

        fn copy(&self, _from: &Path, _to: &Path) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn create_dir(&self, _path: &Path) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn create_dir_all(&self, _path: &Path) -> crate::error::StorageResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_storage_trait_exists() {
        let storage = MockStorage;
        assert!(!storage.exists(Path::new("test.txt")));
    }

    #[test]
    fn test_storage_trait_list_empty() {
        let storage = MockStorage;
        let results = storage.list(Path::new("/")).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_storage_trait_copy() {
        let storage = MockStorage;
        assert!(storage.copy(Path::new("a.txt"), Path::new("b.txt")).is_ok());
    }

    #[test]
    fn test_storage_trait_create_dir() {
        let storage = MockStorage;
        assert!(storage.create_dir(Path::new("test")).is_ok());
    }

    #[test]
    fn test_storage_trait_create_dir_all() {
        let storage = MockStorage;
        assert!(storage.create_dir_all(Path::new("a/b/c")).is_ok());
    }

    #[test]
    fn test_storage_trait_delete_prefix_default() {
        let storage = MockStorage;
        let result = storage.delete_prefix(Path::new("test/"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("delete_prefix"));
    }

    #[test]
    fn test_storage_trait_streaming_reader_default() {
        let storage = MockStorage;
        let result = storage.streaming_reader(
            Path::new("test.txt"),
            crate::metadata::StreamingConfig::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_seek_read_trait() {
        fn assert_seek_read<T: SeekRead>() {}
        assert_seek_read::<std::io::Cursor<Vec<u8>>>();
    }
}
