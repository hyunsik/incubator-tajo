jsPlumb.ready(function() {
  jsPlumb.setRenderMode(jsPlumb.CANVAS);

  var e1 = jsPlumb.addEndpoint(
    "v1",
    {
      anchor:"AutoDefault",
      paintStyle:{
      fillStyle:"blue"
      },
      hoverPaintStyle:{
        fillStyle:"red"
      }
    }
  );

  var e2 = jsPlumb.addEndpoint(
    "v2",
    {
      anchor:"AutoDefault",
      paintStyle:{
        fillStyle:"blue"
      },
      hoverPaintStyle:{
        fillStyle:"red"
      }
    }
  );

  var e4 = jsPlumb.addEndpoint(
    "v2",
    {
      anchor:"AutoDefault",
      paintStyle:{
        fillStyle:"blue"
      },
      hoverPaintStyle:{
        fillStyle:"red"
      }
    }
  );

  var e3 = jsPlumb.addEndpoint(
    "v3",
    {
      anchor:"AutoDefault",
      paintStyle:{
        fillStyle:"blue"
      },
      hoverPaintStyle:{
        fillStyle:"red"
      }
    }
  );

  var con = jsPlumb.connect({
    source:e1,
    target:e2,
    paintStyle:{ strokeStyle:"blue", lineWidth:5  },
    hoverPaintStyle:{ strokeStyle:"red", lineWidth:7 },

    overlays : [ <!-- overlays start -->
      ["Label", {
        cssClass:"l1 component label",
        label : "Hash Partition",
        location:0.5,
        id:"lable",
        events:{
          "click":function(label, evt) {
            alert("clicked on label for connection " + label.component.id);
          }
        }
      }], <!-- label end -->
      ["Arrow", {
        cssClass:"l1arrow",
        location:0.25, width:20,
        events:{
          "click1":function(arrow, evt) {
            alert("clicked on arrow for connection " + arrow.component.id);
          }
        }
      }], <!-- arrow end -->
      ["Arrow", {
        cssClass:"l1arrow",
        location:0.75, width:20,
        events:{
          "click2":function(arrow, evt) {
            alert("clicked on arrow for connection " + arrow.component.id);
          }
        }
      }] <!-- arrow end -->
    ] <!-- overlays end -->
  }); <!-- connect end -->

  var con2 = jsPlumb.connect({
    source:e3,
    target:e4,
    paintStyle:{ strokeStyle:"blue", lineWidth:5  },
    hoverPaintStyle:{ strokeStyle:"red", lineWidth:7 },

    overlays : [ <!-- overlays start -->
      ["Label", {
        cssClass:"l1 component label",
        label : "Hash Partition",
        location:0.5,
        id:"lable",
        events:{
          "click":function(label, evt) {
            alert("clicked on label for connection " + label.component.id);
          }
        }
      }], <!-- label end -->
      ["Arrow", {
        cssClass:"l1arrow",
        location:0.25, width:20,
        events:{
          "click1":function(arrow, evt) {
            alert("clicked on arrow for connection " + arrow.component.id);
          }
        }
      }], <!-- arrow end -->
      ["Arrow", {
        cssClass:"l1arrow",
        location:0.75, width:20,
        events:{
          "click2":function(arrow, evt) {
            alert("clicked on arrow for connection " + arrow.component.id);
          }
        }
      }] <!-- arrow end -->
    ] <!-- overlays end -->
  }); <!-- connect end -->
});