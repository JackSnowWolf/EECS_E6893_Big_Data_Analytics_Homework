var data = [80, 100, 56, 120, 180, 30, 40, 120, 160];
var svgWidth = 500, svgHeight = 300;
// The required padding between bars is 5px.
// The label must locate 2px above the middle of each bar.

var svg = d3.select('svg')
    .attr("width", svgWidth)
    .attr("height", svgHeight);

var barChart = svg.selectAll("rect")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr('x', 20)
    .attr('y', function (d, i) {
        return i * 30
    })
    .attr('height', 25)
    .attr('width', function (d) {
        return d
    })
    .attr("fill", "#CC6450");
