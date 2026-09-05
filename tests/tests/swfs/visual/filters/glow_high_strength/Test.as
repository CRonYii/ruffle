package {
    import flash.display.BitmapData;
    import flash.display.Sprite;
    import flash.filters.GlowFilter;
    import flash.geom.Point;
    import flash.geom.Rectangle;

    public class Test extends Sprite {
        public function Test() {
            var source:BitmapData = new BitmapData(30, 30, true, 0);
            source.fillRect(new Rectangle(10, 10, 10, 10), 0xffffffff);
            for each (var strength:Number in [1, 128, 255]) {
                var bitmap:BitmapData = new BitmapData(30, 30, true, 0);
                bitmap.applyFilter(source, source.rect, new Point(),
                    new GlowFilter(0, 1, 3, 3, strength));
                trace(strength + " source=" + (bitmap.getPixel32(15, 15) == 0xffffffff)
                    + " halo=" + ((bitmap.getPixel32(9, 15) >>> 24) > 0
                        && bitmap.getPixel(9, 15) == 0)
                    + " outside=" + (bitmap.getPixel32(0, 0) == 0));
                bitmap.dispose();
            }
            source.dispose();
        }
    }
}
