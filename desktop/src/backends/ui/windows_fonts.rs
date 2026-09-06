use super::{DeviceFontRenderer, load_fontdb_font};
use fontdb::{Database, Family};
use ruffle_core::backend::ui::FontDefinition;
use ruffle_core::font::FontQuery;

pub(super) fn sort_device_fonts(
    database: &Database,
    query: &FontQuery,
    register: &mut dyn FnMut(FontDefinition),
    renderer: DeviceFontRenderer,
) -> Vec<FontQuery> {
    // Fontdb selects a face, but does not implement Windows font linking.
    // Keep the requested face first; these installed Chinese faces only supply
    // glyphs it lacks. Never replace Latin metrics or bundle system font files.
    let mut fonts = Vec::new();
    let mut seen = Vec::new();
    for (index, name) in [query.name.as_str(), "SimSun", "Microsoft YaHei"]
        .into_iter()
        .enumerate()
    {
        // Leave missing-family substitution to the existing core policy.
        if index > 0 && fonts.is_empty() {
            break;
        }
        let selection = fontdb::Query {
            families: &[Family::Name(name)],
            weight: if query.is_bold {
                fontdb::Weight::BOLD
            } else {
                fontdb::Weight::NORMAL
            },
            style: if query.is_italic {
                fontdb::Style::Italic
            } else {
                fontdb::Style::Normal
            },
            ..Default::default()
        };
        let Some(id) = database.query(&selection) else {
            continue;
        };
        if seen.contains(&id) {
            continue;
        }
        let Some(face) = database.face(id) else {
            continue;
        };
        match load_fontdb_font(name.to_owned(), face, renderer) {
            Ok(definition) => register(definition),
            Err(_) => {
                tracing::warn!("Could not load Windows device font family {name}");
                continue;
            }
        }
        seen.push(id);
        fonts.push(FontQuery::new(
            query.font_type,
            name.to_owned(),
            face.weight > fontdb::Weight::NORMAL,
            face.style != fontdb::Style::Normal,
        ));
    }
    fonts
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruffle_core::font::FontType;

    fn database() -> Database {
        let mut db = Database::new();
        db.load_font_data(include_bytes!("test_fonts/latin.ttf").to_vec());
        db.load_font_data(include_bytes!("test_fonts/latin-bold.ttf").to_vec());
        db.load_font_data(include_bytes!("test_fonts/cjk.ttf").to_vec());
        db
    }

    fn select(db: &Database, family: &str, bold: bool) -> Vec<FontQuery> {
        let query = FontQuery::new(FontType::Device, family.to_owned(), bold, false);
        let mut registered = Vec::new();
        let result = sort_device_fonts(
            db,
            &query,
            &mut |definition| {
                let FontDefinition::FontFile {
                    name,
                    is_bold,
                    is_italic,
                    ..
                } = definition
                else {
                    panic!("expected font file");
                };
                registered.push(FontQuery::new(FontType::Device, name, is_bold, is_italic));
            },
            DeviceFontRenderer::Embedded,
        );
        // Core looks these queries up exactly, so return actual face styles,
        // including when a requested bold fallback only has a regular face.
        assert_eq!(result, registered);
        result
    }

    #[test]
    fn requested_latin_face_precedes_chinese_fallback() {
        let fonts = select(&database(), "Times New Roman", false);
        assert_eq!(
            fonts
                .iter()
                .map(|font| font.name.as_str())
                .collect::<Vec<_>>(),
            ["Times New Roman", "SimSun"]
        );
        assert!(!fonts[0].is_bold);
        assert!(!fonts[1].is_bold);
    }

    #[test]
    fn bold_primary_keeps_actual_regular_fallback_style() {
        let fonts = select(&database(), "Times New Roman", true);
        assert_eq!(fonts.len(), 2);
        assert!(fonts[0].is_bold);
        assert!(!fonts[1].is_bold);
    }

    #[test]
    fn requested_chinese_face_is_not_duplicated() {
        let fonts = select(&database(), "SimSun", false);
        assert_eq!(fonts.len(), 1);
        assert_eq!(fonts[0].name, "SimSun");
    }

    #[test]
    fn missing_fonts_do_not_invent_a_fallback() {
        assert!(select(&Database::new(), "Times New Roman", false).is_empty());
        assert!(select(&database(), "Missing family", false).is_empty());
        let mut db = Database::new();
        db.load_font_data(include_bytes!("test_fonts/latin.ttf").to_vec());
        let fonts = select(&db, "Times New Roman", false);
        assert_eq!(fonts.len(), 1);
        assert_eq!(fonts[0].name, "Times New Roman");
    }
}
