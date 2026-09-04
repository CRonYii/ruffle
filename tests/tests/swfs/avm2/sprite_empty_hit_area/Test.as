package {
    import flash.display.Sprite;
    import flash.events.MouseEvent;

    [SWF(width="100", height="100", frameRate="30")]
    public class Test extends Sprite {
        public function Test() {
            var underlying:Sprite = new Sprite();
            underlying.name = "underlying";
            underlying.graphics.beginFill(0x00ff00);
            underlying.graphics.drawRect(0, 0, 100, 100);
            underlying.graphics.endFill();
            addChild(underlying);

            var overlay:Sprite = new Sprite();
            overlay.name = "overlay";
            overlay.graphics.beginFill(0xff0000, 0.5);
            overlay.graphics.drawRect(0, 0, 100, 100);
            overlay.graphics.endFill();
            overlay.hitArea = new Sprite();
            var child:Sprite = new Sprite();
            child.name = "child";
            child.graphics.beginFill(0x0000ff);
            child.graphics.drawRect(60, 0, 40, 100);
            child.graphics.endFill();
            overlay.addChild(child);
            addChild(overlay);

            var owner:Sprite = new Sprite();
            owner.name = "owner";
            owner.y = 60;
            var designatedArea:Sprite = new Sprite();
            designatedArea.name = "designatedArea";
            designatedArea.graphics.beginFill(0xffff00);
            designatedArea.graphics.drawRect(0, 0, 40, 40);
            designatedArea.graphics.endFill();
            owner.addChild(designatedArea);
            owner.hitArea = designatedArea;
            addChild(owner);

            addEventListener(MouseEvent.MOUSE_DOWN, function(event:MouseEvent):void {
                trace("target=" + event.target.name);
            });
        }
    }
}
