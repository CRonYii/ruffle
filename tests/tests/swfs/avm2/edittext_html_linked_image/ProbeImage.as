package {
    import flash.display.MovieClip;
    public class ProbeImage extends MovieClip {
        public var frameScripts:int = 0;
        public function ProbeImage() {
            addFrameScript(1, function():void { frameScripts++; });
            graphics.beginFill(0x336699);
            graphics.drawRect(0, 0, 20, 20);
            graphics.endFill();
        }
    }
}
