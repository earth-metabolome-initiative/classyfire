diesel::table! {
    molecules (inchikey) {
        inchikey -> Text,
        inchi -> Text,
        state -> Text,
        attempts -> Integer,
        last_error -> Nullable<Text>,
        classification_acquired_at -> Nullable<BigInt>,
        raw_json_zstd -> Nullable<Binary>,
        created_at -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::table! {
    cid_map (cid) {
        cid -> BigInt,
        inchikey -> Text,
    }
}

diesel::table! {
    state_counts (state) {
        state -> Text,
        count -> BigInt,
    }
}

diesel::table! {
    taxonomy_counts (level, label) {
        level -> Text,
        label -> Text,
        count -> BigInt,
    }
}

diesel::table! {
    import_state (source_path) {
        source_path -> Text,
        source_size_bytes -> BigInt,
        source_mtime_epoch -> BigInt,
        last_committed_line -> BigInt,
        last_committed_offset -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::joinable!(cid_map -> molecules (inchikey));

diesel::allow_tables_to_appear_in_same_query!(
    molecules,
    cid_map,
    state_counts,
    taxonomy_counts,
    import_state
);
