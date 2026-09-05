package {
    import flash.display.BitmapData;
    import flash.display.DisplayObject;
    import flash.display.MovieClip;
    import flash.display.Sprite;
    import flash.events.Event;
    import flash.text.TextField;

    public class Test extends MovieClip {
        private var field:TextField = new TextField();
        private var cached:Sprite = new Sprite();
        private var holder:Sprite = new Sprite();
        private var image:DisplayObject;
        private var bitmap:BitmapData = new BitmapData(100, 100, false, 0xffffff);
        private var ticks:int = 0;

        public function Test() {
            field.width = 80;
            field.height = 90;
            field.multiline = true;
            field.htmlText = "<img src='ProbeImage' id='one' width='40' height='20'><img src='ProbeImage' id='two' width='30' height='10'>";
            image = field.getImageReference("one");
            cached.cacheAsBitmap = true;
            cached.addChild(field);
            holder.addChild(cached);
            addChild(holder);
            addEventListener(Event.ENTER_FRAME, check);
        }

        private function capture():void {
            bitmap.fillRect(bitmap.rect, 0xffffff);
            bitmap.draw(holder);
        }

        private function check(event:Event):void {
            if (++ticks === 1) {
                capture();
                trace("first image rendered: " + (bitmap.getPixel(15, 15) === 0x336699));
                trace("second image rendered: " + (bitmap.getPixel(15, 50) === 0x336699));
                trace("spacing remains empty: " + (bitmap.getPixel(5, 5) === 0xffffff));
                image.visible = false;
            } else if (ticks === 2) {
                capture();
                trace("cached ancestor hides image: " + (bitmap.getPixel(15, 15) === 0xffffff));
                trace("other image stays rendered: " + (bitmap.getPixel(15, 50) === 0x336699));
                field.htmlText = "";
            } else if (ticks === 3) {
                capture();
                trace("replacement removes rendering: " + (bitmap.getPixel(15, 50) === 0xffffff));
            }
        }
    }
}
