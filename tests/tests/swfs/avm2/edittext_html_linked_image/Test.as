package {
    import flash.display.DisplayObject;
    import flash.display.MovieClip;
    import flash.events.Event;
    import flash.text.TextField;

    public class Test extends MovieClip {
        public function Test() {
            var field:TextField = new TextField();
            field.width = 320;
            field.height = 180;
            field.multiline = true;
            field.wordWrap = true;
            field.x = 50;
            field.y = 20;
            addChild(field);
            field.htmlText = "<p>A<img src='ProbeImage' id='one' width='40' height='20'>B</p><p><img src='ProbeImage' id='two' width='30' height='10'></p>";
            var one:DisplayObject = field.getImageReference("one");
            var two:DisplayObject = field.getImageReference("two");
            trace("linked instances: " + (one is ProbeImage) + ", " + (two is ProbeImage));
            if (!one || !two) return;
            trace("distinct: " + (one !== two));
            trace("stable: " + (one === field.getImageReference("one")));
            trace("names: " + one.name + ", " + two.name);
            trace("parents: " + one.parent + ", " + two.parent);
            trace("dimensions: " + one.width + ", " + one.height + ", " + two.width + ", " + two.height);
            trace("text: " + field.text.split("\r").join("|"));
            trace("missing: " + field.getImageReference("missing"));
            var height:Number = field.textHeight;
            var x:Number = one.x;
            var y:Number = one.y;
            var control:HtmlControl = new HtmlControl();
            control.name = "one";
            control.InitStage({MSG: {}}, field);
            trace("initialized: " + control.initialized);
            trace("control position: " + (control.x === x && control.y === y));
            trace("hidden: " + !one.visible);
            trace("occupied after hide: " + (field.textHeight === height && two.y > one.y));
            trace("html retains image: " + (field.htmlText.indexOf('<IMG SRC="ProbeImage"') >= 0));
            field.htmlText = "replacement";
            trace("html replacement: " + field.getImageReference("one") + ", " + field.getImageReference("two"));
            trace("old instance survives: " + (one is ProbeImage));
            field.htmlText = "<img src='ProbeImage' id='new'>";
            var fresh:DisplayObject = field.getImageReference("new");
            trace("new instance: " + (fresh is ProbeImage && fresh !== one));
            trace("natural dimensions: " + fresh.width + ", " + fresh.height);
            field.text = "plain";
            trace("text replacement: " + field.getImageReference("new"));

            checkPlacement("default", "", 10, 10, 58);
            checkPlacement("zero", "hspace='0' vspace='0'", 2, 2, 42);
            checkPlacement("spacing", "hspace='3' vspace='4'", 5, 6, 48);
            checkPlacement("right", "align='right' hspace='3' vspace='4'", 277, 6, 2);
            field = freshField();
            field.htmlText = "A<img src='ProbeImage' id='one' width='40' height='20'>B";
            one = field.getImageReference("one");
            trace("midtext below line: " + (Math.abs(one.y - 10 - field.getLineMetrics(0).height) < 0.1));
            field.htmlText = "<img src='ProbeImage' id='one' width='40' height='20'><img src='ProbeImage' id='two' width='30' height='10'>AB";
            trace("stacked: " + field.getImageReference("one").y + ", " + field.getImageReference("two").y);
            var html:String = "<img src='ProbeImage' id='one'>AB";
            field.htmlText = html;
            one = field.getImageReference("one");
            field.htmlText = html;
            trace("same source new instance: " + (one !== field.getImageReference("one")));
            field.text = field.text;
            trace("same text removes image: " + field.getImageReference("one"));

            field = freshField();
            field.width = 120;
            var words:String = new Array(60).join("word ");
            field.htmlText = "<img src='ProbeImage' id='one' width='40' height='40'>" + words;
            trace("wrap excludes image: " + (field.getCharBoundaries(field.getLineOffset(1)).x === 58));
            trace("wrap below image: " + (field.getCharBoundaries(field.getLineOffset(field.numLines - 1)).x === 2));
            one = field.getImageReference("one");
            field.height = 15;
            field.scrollV = 2;
            trace("scroll keeps reference position: " + one.x + ", " + one.y);
            field.replaceText(0, 1, "");
            trace("remove anchor: " + field.getImageReference("one"));

            var animated:ProbeImage = fresh;
            var ticks:int = 0;
            addEventListener(Event.ENTER_FRAME, function(event:Event):void {
                if (++ticks === 3) {
                    trace("orphan timeline advances: " + (animated.frameScripts > 0));
                }
            });
        }

        private function freshField():TextField {
            var field:TextField = new TextField();
            field.width = 320;
            field.height = 180;
            field.multiline = true;
            field.wordWrap = true;
            return field;
        }

        private function checkPlacement(label:String, attributes:String, x:Number, y:Number, textX:Number):void {
            var field:TextField = freshField();
            field.htmlText = "<img src='ProbeImage' id='one' width='40' height='20' " + attributes + ">AB";
            var image:DisplayObject = field.getImageReference("one");
            trace(label + " position: " + (image.x === x && image.y === y));
            trace(label + " exclusion: " + (field.getCharBoundaries(1).x === textX));
            trace(label + " anchor bounds: " + field.getCharBoundaries(0));
            var height:Number = field.textHeight;
            field.text = "AB";
            trace(label + " text height excludes image: " + (field.textHeight === height));
        }
    }
}

import flash.display.MovieClip;
import flash.text.TextField;

class HtmlControl extends MovieClip {
    public var initialized:Boolean = false;
    private var oBDing:*;
    private var mcMSGObj:*;

    // The original htmlMC initialization sequence: a null reference prevents InitCode.
    public function InitStage(building:*, field:TextField):void {
        this.oBDing = building;
        var image:* = field.getImageReference(this.name);
        image.visible = false;
        this.x = image.x;
        this.y = image.y;
        mcMSGObj = building["MSG"];
        this.InitCode();
    }

    protected function InitCode():void {
        initialized = true;
    }
}
