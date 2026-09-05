package {
    import flash.display.Sprite;
    import flash.filters.GlowFilter;

    public class Test extends Sprite {
        public function Test() {
            var child:Sprite = new Sprite();
            addChild(child);
            for each (var strength:Number in [0, 1, 127.5, 128, 255]) {
                child.filters = [new GlowFilter(0, 1, 3, 3, strength)];
                trace(strength + " -> " + GlowFilter(child.filters[0]).strength);
            }
        }
    }
}
