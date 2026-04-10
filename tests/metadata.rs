use ishikari::pmtiles::Metadata;

fn places_metadata_fixture() -> &'static [u8] {
    include_bytes!("fixtures/metadata/places.pmtiles.json")
}

fn v4_metadata_fixture() -> &'static [u8] {
    include_bytes!("fixtures/metadata/v4.pmtiles.json")
}

#[test]
fn parses_vector_layers_and_tilestats_from_fixture() {
    let metadata: Metadata = serde_json::from_slice(places_metadata_fixture())
        .expect("places metadata fixture should parse");

    assert_eq!(metadata.name.as_deref(), Some("places.pmtiles"));
    assert_eq!(metadata.description.as_deref(), Some("places.pmtiles"));
    assert_eq!(metadata.version.as_deref(), Some("2"));

    let vector_layers = metadata.vector_layers();
    assert_eq!(vector_layers.len(), 1);
    assert_eq!(vector_layers[0].id, "place");
    assert_eq!(vector_layers[0].minzoom, Some(0));
    assert_eq!(vector_layers[0].maxzoom, Some(15));
    assert_eq!(
        vector_layers[0].fields.get("@name").map(String::as_str),
        Some("String")
    );

    let tilestats = metadata.tilestats().expect("tilestats should be present");
    assert_eq!(tilestats.layer_count, 1);
    assert_eq!(tilestats.layers.len(), 1);
    assert_eq!(tilestats.layers[0].layer, "place");
    assert_eq!(tilestats.layers[0].geometry, "Point");
}

#[test]
fn preserves_unknown_metadata_fields_in_other() {
    let metadata: Metadata = serde_json::from_slice(places_metadata_fixture())
        .expect("places metadata fixture should parse");

    let other = metadata.other();
    assert_eq!(
        other.get("type").and_then(|value| value.as_str()),
        Some("overlay")
    );
    assert!(other.contains_key("strategies"));
    assert!(other.contains_key("tippecanoe_decisions"));
    assert!(other.contains_key("generator"));
    assert!(other.contains_key("generator_options"));
    assert!(other.contains_key("antimeridian_adjusted_bounds"));
    assert!(!other.contains_key("format"));
    assert!(!other.contains_key("tilestats"));
    assert!(!other.contains_key("vector_layers"));
}

#[test]
fn parses_v4_metadata_fixture() {
    let metadata: Metadata =
        serde_json::from_slice(v4_metadata_fixture()).expect("v4 metadata fixture should parse");

    assert_eq!(metadata.name.as_deref(), Some("Protomaps Basemap"));
    assert_eq!(
        metadata.description.as_deref(),
        Some("Basemap layers derived from OpenStreetMap and Natural Earth")
    );
    assert_eq!(
        metadata.attribution.as_deref(),
        Some(
            "<a href=\"https://www.openstreetmap.org/copyright\" target=\"_blank\">&copy; OpenStreetMap</a>"
        )
    );
    assert_eq!(metadata.version.as_deref(), Some("4.14.5"));

    let vector_layers = metadata.vector_layers();
    assert_eq!(vector_layers.len(), 9);
    assert_eq!(vector_layers[0].id, "boundaries");
    assert_eq!(vector_layers[5].id, "places");
    assert_eq!(vector_layers[8].id, "water");
    assert!(metadata.tilestats().is_none());

    let other = metadata.other();
    assert_eq!(
        other.get("type").and_then(|value| value.as_str()),
        Some("baselayer")
    );
    assert!(other.contains_key("planetiler:version"));
    assert!(other.contains_key("planetiler:githash"));
    assert!(other.contains_key("pgf:devanagari:name"));
}
