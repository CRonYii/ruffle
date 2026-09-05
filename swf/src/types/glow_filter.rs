use crate::{BlurFilter, BlurFilterFlags, Color, Fixed8, Fixed16, Rectangle, Twips};
use bitflags::bitflags;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlowFilter {
    pub color: Color,
    pub blur_x: Fixed16,
    pub blur_y: Fixed16,
    /// Raw unsigned 8.8 bits, retained in `Fixed8` for SWF read/write compatibility.
    /// Use `strength()` and `set_strength()` for numeric conversions.
    pub strength: Fixed8,
    pub flags: GlowFilterFlags,
}

impl GlowFilter {
    pub fn strength(&self) -> f64 {
        f64::from(self.strength.get() as u16) / 256.0
    }

    pub fn set_strength(&mut self, strength: f64) {
        self.strength = Fixed8::from_bits((strength * 256.0) as u16 as i16);
    }

    #[inline]
    pub fn is_inner(&self) -> bool {
        self.flags.contains(GlowFilterFlags::INNER_GLOW)
    }

    #[inline]
    pub fn is_knockout(&self) -> bool {
        self.flags.contains(GlowFilterFlags::KNOCKOUT)
    }

    #[inline]
    pub fn composite_source(&self) -> bool {
        self.flags.contains(GlowFilterFlags::COMPOSITE_SOURCE)
    }

    #[inline]
    pub fn num_passes(&self) -> u8 {
        (self.flags & GlowFilterFlags::PASSES).bits()
    }

    pub fn scale(&mut self, x: f32, y: f32) {
        self.blur_x = BlurFilter::scale_blur(self.blur_x, x);
        self.blur_y = BlurFilter::scale_blur(self.blur_y, y);
    }

    pub fn calculate_dest_rect(&self, source_rect: Rectangle<Twips>) -> Rectangle<Twips> {
        // TODO: Inner might not need this. Docs suggest it doesn't care about source rect, but rather source *size*?
        self.inner_blur_filter().calculate_dest_rect(source_rect)
    }

    pub fn inner_blur_filter(&self) -> BlurFilter {
        BlurFilter {
            blur_x: self.blur_x,
            blur_y: self.blur_y,
            flags: BlurFilterFlags::from_passes(self.num_passes()),
        }
    }
}

bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct GlowFilterFlags: u8 {
        const INNER_GLOW       = 1 << 7;
        const KNOCKOUT         = 1 << 6;
        const COMPOSITE_SOURCE = 1 << 5;
        const PASSES           = 0b11111;
    }
}

impl GlowFilterFlags {
    #[inline]
    pub fn from_passes(num_passes: u8) -> Self {
        let flags = Self::from_bits_retain(num_passes);
        debug_assert_eq!(flags & Self::PASSES, flags);
        flags
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_strength_round_trip() {
        for (raw, expected) in [
            (0x0000u16, 0.0),
            (0x0100, 1.0),
            (0x7fff, 127.99609375),
            (0x8000, 128.0),
            (0xff00, 255.0),
            (0xffff, 255.99609375),
        ] {
            let mut filter = GlowFilter {
                color: Color::BLACK,
                blur_x: Fixed16::from_f32(3.0),
                blur_y: Fixed16::from_f32(3.0),
                strength: Fixed8::from_bits(raw as i16),
                flags: GlowFilterFlags::COMPOSITE_SOURCE | GlowFilterFlags::from_passes(1),
            };
            assert_eq!(filter.strength(), expected, "raw strength {raw:04x}");
            filter.set_strength(expected);
            assert_eq!(filter.strength.get() as u16, raw);
        }
    }
}
