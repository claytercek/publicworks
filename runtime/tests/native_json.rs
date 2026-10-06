use publicworks_runtime::{StorageError, decode_native_json};
use serde_json::{Value, json};

const NUMBER_KEY: &str = "$serde_json::private::Number";
const RAW_VALUE_KEY: &str = "$serde_json::private::RawValue";

fn assert_other(bytes: &[u8]) {
    assert!(
        matches!(decode_native_json(bytes), Err(StorageError::Other(_))),
        "expected invalid native JSON: {}",
        String::from_utf8_lossy(bytes)
    );
}

#[test]
fn accepts_native_number_boundaries_and_roundtrippable_floats() {
    let value = decode_native_json(
        br#"[-9223372036854775808,18446744073709551615,1.7976931348623157e+308,5e-324]"#,
    )
    .unwrap();
    assert_eq!(
        value,
        json!([i64::MIN, u64::MAX, f64::MAX, f64::from_bits(1)])
    );

    for expected in [0.1, -0.0, f64::MAX, f64::from_bits(1)] {
        let encoded = serde_json::to_vec(&expected).unwrap();
        let actual = decode_native_json(&encoded).unwrap().as_f64().unwrap();
        assert_eq!(actual.to_bits(), expected.to_bits());
    }
}

#[test]
fn reserved_number_and_raw_value_keys_remain_objects_everywhere() {
    for key in [NUMBER_KEY, RAW_VALUE_KEY] {
        let root = format!(r#"{{"{key}":"123"}}"#);
        let value = decode_native_json(root.as_bytes()).unwrap();
        let object = value
            .as_object()
            .expect("reserved-key root must be an object");
        assert_eq!(object.len(), 1);
        assert_eq!(object.get(key), Some(&Value::String("123".into())));

        let nested = format!(r#"{{"nested":{{"{key}":"nested"}},"array":[{{"{key}":"array"}}]}}"#);
        let value = decode_native_json(nested.as_bytes()).unwrap();
        assert_eq!(value["nested"][key], "nested");
        assert!(value["nested"].is_object());
        assert_eq!(value["array"][0][key], "array");
        assert!(value["array"][0].is_object());
    }
}

#[test]
fn rejects_invalid_trailing_out_of_domain_and_nonfinite_input() {
    for bytes in [
        b"{".as_slice(),
        b"[1,]".as_slice(),
        b"null true".as_slice(),
        b"1e400".as_slice(),
        b"NaN".as_slice(),
        b"Infinity".as_slice(),
        b"-Infinity".as_slice(),
    ] {
        assert_other(bytes);
    }

    // These spellings are retained only when arbitrary_precision is unified in.
    // If retained, the shared decoder must reject them rather than expose a
    // number outside the native/canonical storage domain.
    for bytes in [
        b"18446744073709551616".as_slice(),
        b"1.000".as_slice(),
        b"0.123456789012345678901".as_slice(),
    ] {
        if serde_json::from_slice::<Value>(bytes)
            .is_ok_and(|value| value.to_string().as_bytes() == bytes)
        {
            assert_other(bytes);
        }
    }
}

fn nested_arrays(depth: usize) -> Vec<u8> {
    format!("{}null{}", "[".repeat(depth), "]".repeat(depth)).into_bytes()
}

#[test]
fn accepts_exact_depth_limit_and_rejects_one_more_level() {
    let value = decode_native_json(&nested_arrays(64)).unwrap();
    let mut current = &value;
    for _ in 0..64 {
        let values = current.as_array().expect("expected another array level");
        assert_eq!(values.len(), 1);
        current = &values[0];
    }
    assert!(current.is_null());

    assert_other(&nested_arrays(65));
}
