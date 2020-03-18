var d3 = require("d3"),
    cloud = require("../");

var fill = d3.scaleOrdinal(d3.schemeCategory10);

// console.log([
//   "Hello", "world", "normally", "you", "want", "more", "words",
//   "than", "this", "bundle", "padding", "font", "impact", "function"].map(function(d) {
//   return {text: d, size: 10 + Math.random() * 90, test: "haha"};
// }));

var layout = cloud()
    .size([1200, 500])
    // .words([
    //   "Hello", "world", "normally", "you", "want", "more", "words",
    //   "than", "this", "bundle", "padding", "font", "impact", "function"].map(function(d) {
    //   return {text: d, size: 10 + Math.random() * 90, test: "haha"};
    // }))
    .words(tokens)
    .padding(5)
    .rotate(function() { return (~~(Math.random() * 6) - 3) * 30;})
    .font("Impact")
    // .style("fill", function(d, i) { return fill(i); })
    
    .fontSize(function(d) { return d.size; })
    .on("end", draw);

layout.start();

// var fill = d3.scaleOrdinal(d3.schemeCategory10);
// console.log(fill);
function draw(words) {
  d3.select("body").append("svg")
      .attr("width", layout.size()[0])
      .attr("height", layout.size()[1])
    .append("g")
      .attr("transform", "translate(" + layout.size()[0] / 2 + "," + layout.size()[1] / 2 + ")")
    .selectAll("text")
      .data(words)
    .enter().append("text")
    .attr("fill",function(d,i){return fill(i);})
    // .attr("fill", function(d){return fill(d) })
      .style("font-size", function(d) { return d.size + "px"; })
      .style("font-family", "Impact")
      .attr("text-anchor", "middle")
      .attr("transform", function(d) {
        return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
      })
      .text(function(d) { return d.text; });
}
