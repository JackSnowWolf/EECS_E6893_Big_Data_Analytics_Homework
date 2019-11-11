var data = [80, 100, 56, 120, 180, 30, 40, 120, 160];
var svgWidth = 500, svgHeight = 300;
// The required padding between bars is 5px.
// The label must locate 2px above the middle of each bar.

var svg = d3.select('svg')
    .attr("width", svgWidth)
    .attr("height", svgHeight);

const barPadding = 5;
const barWidth = svgWidth / data.length - barPadding;

function translateBarHelper(d, i) {
    return "translate(" + (barWidth + barPadding) * i + ","
        + (svgHeight - d) + ")";
}

function translateTextHelper(d, i) {
    return "translate(" + ((barWidth + barPadding) * i + barWidth / 2) + ","
        + (svgHeight - d - 2) + ")";
}

var barChart = svg.selectAll("rect")
    .data(data)
    .enter();

barChart.append("rect")
    .attr("class", "bar")
    .attr("height", function (d) {
        return d;
    })
    .attr("width", barWidth)
    .attr("transform", translateBarHelper)
    .attr("fill", "#CC6450");

barChart.append("text")
    .text(function (d) {
        return d;
    }).attr("transform", translateTextHelper)
    .style("text-anchor", "middle");