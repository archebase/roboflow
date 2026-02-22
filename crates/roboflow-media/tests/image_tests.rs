// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for image decoding functionality.

use roboflow_media::ImageData;
use roboflow_media::image::{
    ImageDecoderConfig, ImageDecoderFactory, ImageFormat, decode_compressed_image,
    decode_image_to_rgb, decode_images_parallel, decode_images_parallel_with_dims, decode_to_rgb,
};

/// Helper to create a minimal valid JPEG image.
fn create_test_jpeg(width: u32, height: u32) -> Vec<u8> {
    let rgb_data: Vec<u8> = (0..width * height * 3)
        .map(|i| ((i * 17) % 256) as u8)
        .collect();

    let img = image::RgbImage::from_raw(width, height, rgb_data).expect("Invalid dimensions");
    let mut jpeg_buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut jpeg_buf, image::ImageFormat::Jpeg)
        .expect("JPEG encoding failed");
    jpeg_buf.into_inner()
}

/// Helper to create a minimal valid PNG image.
fn create_test_png(width: u32, height: u32) -> Vec<u8> {
    let rgb_data: Vec<u8> = (0..width * height * 3)
        .map(|i| ((i * 17) % 256) as u8)
        .collect();

    let img = image::RgbImage::from_raw(width, height, rgb_data).expect("Invalid dimensions");
    let mut png_buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut png_buf, image::ImageFormat::Png)
        .expect("PNG encoding failed");
    png_buf.into_inner()
}

#[test]
fn test_decode_jpeg_to_rgb() {
    let jpeg_data = create_test_jpeg(64, 48);
    let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);

    assert!(result.is_ok(), "JPEG decoding should succeed");
    let decoded = result.unwrap();
    assert_eq!(decoded.width, 64);
    assert_eq!(decoded.height, 48);
    assert_eq!(decoded.data.len(), 64 * 48 * 3);
}

#[test]
fn test_decode_png_to_rgb() {
    let png_data = create_test_png(32, 32);
    let result = decode_compressed_image(&png_data, ImageFormat::Png);

    assert!(result.is_ok(), "PNG decoding should succeed");
    let decoded = result.unwrap();
    assert_eq!(decoded.width, 32);
    assert_eq!(decoded.height, 32);
    assert_eq!(decoded.data.len(), 32 * 32 * 3);
}

#[test]
fn test_decode_invalid_format() {
    let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let result = decode_compressed_image(&invalid_data, ImageFormat::Jpeg);

    assert!(result.is_err(), "Decoding invalid data should fail");
}

#[test]
fn test_decode_jpeg_with_factory() {
    let jpeg_data = create_test_jpeg(128, 96);
    let config = ImageDecoderConfig::default();
    let mut factory = ImageDecoderFactory::new(&config);

    let decoder = factory.get_decoder();
    let result = decoder.decode(&jpeg_data, ImageFormat::Jpeg);

    assert!(result.is_ok());
    let decoded = result.unwrap();
    assert_eq!(decoded.width, 128);
    assert_eq!(decoded.height, 96);
}

#[test]
fn test_decode_image_to_rgb_function() {
    let jpeg_data = create_test_jpeg(50, 50);
    let encoded_image = ImageData::encoded(50, 50, jpeg_data);

    let (width, height, data) = decode_image_to_rgb(&encoded_image).unwrap();

    assert_eq!(width, 50);
    assert_eq!(height, 50);
    assert_eq!(data.len(), 50 * 50 * 3);
}

#[test]
fn test_parallel_decode_with_dims() {
    let jpeg_data_1 = create_test_jpeg(32, 32);
    let jpeg_data_2 = create_test_jpeg(64, 64);

    let images: Vec<(&[u8], ImageFormat, u32, u32)> = vec![
        (&jpeg_data_1, ImageFormat::Jpeg, 32, 32),
        (&jpeg_data_2, ImageFormat::Jpeg, 64, 64),
    ];

    let results = decode_images_parallel_with_dims(&images);

    assert_eq!(results.len(), 2);
    assert!(results[0].is_some(), "First image should decode");
    assert!(results[1].is_some(), "Second image should decode");

    let decoded1 = results[0].as_ref().unwrap();
    assert_eq!(decoded1.width, 32);
    assert_eq!(decoded1.height, 32);
}

#[test]
fn test_parallel_decode_single_image() {
    let jpeg_data = create_test_jpeg(64, 64);
    let images: Vec<(&[u8], ImageFormat)> = vec![(&jpeg_data, ImageFormat::Jpeg)];

    let results = decode_images_parallel(&images);

    assert_eq!(results.len(), 1);
    assert!(results[0].is_some(), "Single image decode should succeed");
    let decoded = results[0].as_ref().unwrap();
    assert_eq!(decoded.width, 64);
    assert_eq!(decoded.height, 64);
}

#[test]
fn test_parallel_decode_multiple_images() {
    let jpeg_data_1 = create_test_jpeg(32, 32);
    let jpeg_data_2 = create_test_jpeg(64, 64);
    let png_data = create_test_png(128, 96);

    let images: Vec<(&[u8], ImageFormat)> = vec![
        (&jpeg_data_1, ImageFormat::Jpeg),
        (&jpeg_data_2, ImageFormat::Jpeg),
        (&png_data, ImageFormat::Png),
    ];

    let results = decode_images_parallel(&images);

    assert_eq!(results.len(), 3);
    for result in &results {
        assert!(result.is_some(), "All images should decode successfully");
    }
}

#[test]
fn test_image_format_detection() {
    use roboflow_media::image::detect_image_format;

    let jpeg_data = create_test_jpeg(32, 32);
    let png_data = create_test_png(32, 32);

    assert_eq!(
        detect_image_format(&jpeg_data),
        ImageFormat::Jpeg,
        "Should detect JPEG format"
    );
    assert_eq!(
        detect_image_format(&png_data),
        ImageFormat::Png,
        "Should detect PNG format"
    );
}

#[test]
fn test_can_passthrough() {
    use roboflow_media::image::can_passthrough;

    let jpeg_data = create_test_jpeg(32, 32);
    let png_data = create_test_png(32, 32);
    let unknown_data = vec![0x00, 0x01, 0x02, 0x03];

    // JPEG should be passthrough-eligible
    assert!(can_passthrough(&jpeg_data));

    // PNG should not be passthrough-eligible (only JPEG)
    assert!(!can_passthrough(&png_data));

    // Unknown format should not be passthrough-eligible
    assert!(!can_passthrough(&unknown_data));
}

#[test]
fn test_image_data_from_decoded() {
    let jpeg_data = create_test_jpeg(100, 100);
    let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg).unwrap();

    // Create ImageData from decoded RGB
    let image_data = ImageData::new(100, 100, result.data);
    assert_eq!(image_data.width, 100);
    assert_eq!(image_data.height, 100);
    assert!(!image_data.is_encoded);
    assert!(image_data.validate());
}

#[test]
fn test_empty_image_data() {
    let data = vec![0u8; 100 * 100 * 3];
    let image = ImageData::new(100, 100, data);
    assert!(image.validate());
    assert_eq!(image.pixel_count(), 10000);
    assert_eq!(image.rgb_size(), 30000);
}

#[test]
fn test_encoded_image_data() {
    let jpeg_data = create_test_jpeg(50, 50);
    let image = ImageData::encoded(50, 50, jpeg_data);
    assert!(image.is_encoded);
    assert!(image.validate()); // Encoded data always validates
}

#[test]
fn test_depth_image_data() {
    let depth_data = vec![0u8; 640 * 480 * 2]; // 16-bit depth
    let image = ImageData::depth(640, 480, depth_data);
    assert!(image.is_depth);
    assert!(!image.is_encoded);
}

#[test]
fn test_image_data_with_timestamp() {
    let data = vec![0u8; 320 * 240 * 3];
    let timestamp = 1234567890;
    let image = ImageData::with_timestamp(320, 240, data, timestamp);
    assert_eq!(image.original_timestamp, timestamp);
}

#[test]
fn test_decode_to_rgb_from_encoded() {
    let jpeg_data = create_test_jpeg(100, 100);
    let encoded_image = ImageData::encoded(100, 100, jpeg_data);

    let result = decode_to_rgb(&encoded_image);
    assert!(result.is_some(), "Encoded image should decode to RGB");
    let (width, height, data) = result.unwrap();
    assert_eq!(width, 100);
    assert_eq!(height, 100);
    assert_eq!(data.len(), 100 * 100 * 3);
}

#[test]
fn test_decode_to_rgb_from_raw() {
    let data = vec![128u8; 50 * 50 * 3];
    let raw_image = ImageData::new(50, 50, data);

    let result = decode_to_rgb(&raw_image);
    assert!(
        result.is_some(),
        "Raw RGB image should return data directly"
    );
    let (width, height, data) = result.unwrap();
    assert_eq!(width, 50);
    assert_eq!(height, 50);
    assert_eq!(data.len(), 50 * 50 * 3);
}

#[test]
fn test_grayscale_jpeg_decoding() {
    // Create a grayscale JPEG (single channel)
    let gray_data: Vec<u8> = (0..32 * 32).map(|i| i as u8).collect();
    let gray_img = image::GrayImage::from_raw(32, 32, gray_data).expect("Invalid dimensions");
    let mut jpeg_buf = std::io::Cursor::new(Vec::new());
    gray_img
        .write_to(&mut jpeg_buf, image::ImageFormat::Jpeg)
        .expect("JPEG encoding failed");
    let jpeg_data = jpeg_buf.into_inner();

    let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);
    assert!(result.is_ok(), "Grayscale JPEG should decode to RGB");
    let decoded = result.unwrap();
    assert_eq!(decoded.width, 32);
    assert_eq!(decoded.height, 32);
}

#[test]
fn test_large_image_decoding() {
    // Test with larger image to ensure no buffer issues
    let jpeg_data = create_test_jpeg(1920, 1080);
    let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);

    assert!(result.is_ok());
    let decoded = result.unwrap();
    assert_eq!(decoded.width, 1920);
    assert_eq!(decoded.height, 1080);
    assert_eq!(decoded.data.len(), 1920 * 1080 * 3);
}

#[test]
fn test_decode_corrupted_jpeg() {
    // Create data that starts with JPEG magic but is corrupted
    let mut data = vec![0xFF, 0xD8, 0xFF]; // JPEG magic bytes
    data.extend_from_slice(&[0x00; 100]); // Invalid JPEG data

    let result = decode_compressed_image(&data, ImageFormat::Jpeg);
    assert!(result.is_err(), "Corrupted JPEG should fail to decode");
}

#[test]
fn test_image_format_magic_bytes() {
    // Test JPEG magic bytes detection
    let jpeg_data = create_test_jpeg(32, 32);
    assert_eq!(jpeg_data[0], 0xFF);
    assert_eq!(jpeg_data[1], 0xD8);
    assert_eq!(jpeg_data[2], 0xFF);

    // Test PNG magic bytes detection
    let png_data = create_test_png(32, 32);
    assert_eq!(png_data[0], 0x89);
    assert_eq!(png_data[1], 0x50);
    assert_eq!(png_data[2], 0x4E);
    assert_eq!(png_data[3], 0x47);
}

#[test]
fn test_detect_jpeg_function() {
    use roboflow_media::image::format::detect_jpeg;
    use roboflow_media::image::format::detect_png;

    let jpeg_data = create_test_jpeg(32, 32);
    let png_data = create_test_png(32, 32);

    assert!(detect_jpeg(&jpeg_data));
    assert!(!detect_png(&jpeg_data));

    assert!(detect_png(&png_data));
    assert!(!detect_jpeg(&png_data));
}

#[test]
fn test_factory_reuse_decoder() {
    let config = ImageDecoderConfig::default();
    let mut factory = ImageDecoderFactory::new(&config);

    let jpeg_data = create_test_jpeg(64, 64);

    // Decode multiple times with the same factory
    let decoder = factory.get_decoder();
    let result1 = decoder.decode(&jpeg_data, ImageFormat::Jpeg);
    assert!(result1.is_ok());

    let decoder2 = factory.get_decoder();
    let result2 = decoder2.decode(&jpeg_data, ImageFormat::Jpeg);
    assert!(result2.is_ok());
}

#[test]
fn test_memory_alignment_for_decoded_images() {
    let jpeg_data = create_test_jpeg(128, 128);
    let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);

    assert!(result.is_ok());
    let decoded = result.unwrap();

    // Check that the data is properly aligned for SIMD operations
    let ptr = decoded.data.as_ptr();
    assert_eq!(ptr.align_offset(4), 0, "Data should be 4-byte aligned");
}

#[test]
fn test_batch_decode_with_mixed_formats() {
    let jpeg_data_1 = create_test_jpeg(32, 32);
    let png_data = create_test_png(32, 32);
    let jpeg_data_2 = create_test_jpeg(64, 64);

    let images: Vec<(&[u8], ImageFormat)> = vec![
        (&jpeg_data_1, ImageFormat::Jpeg),
        (&png_data, ImageFormat::Png),
        (&jpeg_data_2, ImageFormat::Jpeg),
    ];

    let results = decode_images_parallel(&images);

    assert_eq!(results.len(), 3);
    for result in &results {
        assert!(
            result.is_some(),
            "Mixed format images should decode successfully"
        );
    }
}

#[test]
fn test_concurrent_decode_with_different_sizes() {
    let data1 = create_test_jpeg(16, 16);
    let data2 = create_test_jpeg(32, 32);
    let data3 = create_test_jpeg(64, 64);
    let data4 = create_test_jpeg(128, 128);

    let images: Vec<(&[u8], ImageFormat, u32, u32)> = vec![
        (&data1, ImageFormat::Jpeg, 16, 16),
        (&data2, ImageFormat::Jpeg, 32, 32),
        (&data3, ImageFormat::Jpeg, 64, 64),
        (&data4, ImageFormat::Jpeg, 128, 128),
    ];

    let results = decode_images_parallel_with_dims(&images);

    assert_eq!(results.len(), 4);
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_some(), "Image {} should decode successfully", i);
    }
}

#[test]
fn test_image_data_new_rgb_validation() {
    // Test new_rgb with correct size
    let data = vec![0u8; 100 * 100 * 3];
    let result = ImageData::new_rgb(100, 100, data);
    assert!(result.is_ok());

    // Test new_rgb with incorrect size
    let data = vec![0u8; 100 * 100 * 2]; // Wrong size
    let result = ImageData::new_rgb(100, 100, data);
    assert!(result.is_err());
}

#[test]
fn test_image_data_is_rgb() {
    let rgb_data = vec![0u8; 100 * 100 * 3];
    let rgb_image = ImageData::new(100, 100, rgb_data);
    assert!(rgb_image.is_rgb());

    let encoded_data = vec![0u8; 1000];
    let encoded_image = ImageData::encoded(100, 100, encoded_data);
    assert!(!encoded_image.is_rgb());
}

#[test]
fn test_decode_small_images() {
    // Test very small images
    for size in [8, 16, 24, 31] {
        let jpeg_data = create_test_jpeg(size, size);
        let result = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);
        assert!(result.is_ok(), "Should decode {}x{} image", size, size);
        let decoded = result.unwrap();
        assert_eq!(decoded.width, size);
        assert_eq!(decoded.height, size);
    }
}

#[test]
fn test_multiple_decode_calls_same_data() {
    let jpeg_data = create_test_jpeg(64, 64);

    // Decode the same data multiple times
    let result1 = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);
    let result2 = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);
    let result3 = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg);

    assert!(result1.is_ok());
    assert!(result2.is_ok());
    assert!(result3.is_ok());

    let img1 = result1.unwrap();
    let img2 = result2.unwrap();
    let img3 = result3.unwrap();

    // All should have same dimensions
    assert_eq!(img1.width, img2.width);
    assert_eq!(img2.width, img3.width);
}

#[test]
fn test_detect_image_format_unknown() {
    let unknown_data = vec![0x00, 0x01, 0x02, 0x03];
    let format = roboflow_media::image::detect_image_format(&unknown_data);
    assert_eq!(format, ImageFormat::Unknown);
}

#[test]
fn test_image_data_pixel_count() {
    let image = ImageData::new(1920, 1080, vec![0u8; 1920 * 1080 * 3]);
    assert_eq!(image.pixel_count(), 1920 * 1080);
}

#[test]
fn test_image_data_validate() {
    // Valid RGB data
    let valid_data = vec![0u8; 100 * 100 * 3];
    let valid_image = ImageData::new(100, 100, valid_data);
    assert!(valid_image.validate());

    // Invalid RGB data (wrong size)
    let invalid_data = vec![0u8; 100 * 100 * 2];
    let invalid_image = ImageData::new(100, 100, invalid_data);
    assert!(!invalid_image.validate());

    // Encoded data always validates
    let encoded_data = vec![0u8; 1000];
    let encoded_image = ImageData::encoded(100, 100, encoded_data);
    assert!(encoded_image.validate());
}

#[test]
fn test_image_data_depth_validation() {
    let depth_data = vec![0u8; 640 * 480 * 2];
    let depth_image = ImageData::depth(640, 480, depth_data);
    assert!(depth_image.is_depth);
    assert!(!depth_image.is_encoded);
}
