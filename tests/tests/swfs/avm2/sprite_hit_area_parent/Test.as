package {
    import flash.display.Shape;
    import flash.display.Sprite;
    import flash.events.MouseEvent;

    [SWF(width="100", height="100", frameRate="30")]
    public class Test extends Sprite {
        public function Test() {
            var background:Sprite = new Sprite();
            background.name = "background";
            background.graphics.beginFill(0xffffff);
            background.graphics.drawRect(0, 0, 100, 100);
            addChild(background);

            var city:Sprite = new Sprite();
            city.name = "city";
            city.x = 10;
            city.y = 10;
            addChild(city);

            var skin:Sprite = new Sprite();
            skin.name = "skin";
            var artwork:Shape = new Shape();
            artwork.graphics.beginFill(0x0000ff);
            artwork.graphics.drawRect(0, 0, 30, 30);
            skin.addChild(artwork);
            skin.hitArea = new Sprite();
            city.addChild(skin);

            addEventListener(MouseEvent.MOUSE_DOWN, report);
            addEventListener(MouseEvent.CLICK, report);
        }

        private function report(event:MouseEvent):void {
            trace(event.type + " target=" + event.target.name);
        }
    }
}
