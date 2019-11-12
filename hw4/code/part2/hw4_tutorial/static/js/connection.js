function connection(nodes, edges) {
    var width = 1440;
    var height = 1080;


    var svg = d3.select("body")
        .append("svg")
        .attr(/* TO FINISH */)
        .attr(/* TO FINISH */);

    var force = d3.layout.force()
        .nodes(nodes)
        .links(edges)
        .size([width, height])
        .linkDistance(300)
        .charge(-200);

    force.start();

    console.log(nodes);
    console.log(edges);


    var svg_edges = svg.selectAll("line")
        .data(/* TO FINISH */)
        .enter()
        .append(/* TO FINISH */)
        .style("stroke", "#ccc")
        .style("stroke-width", 1);

    var color = d3.scale.category20();


    var svg_nodes = svg.selectAll("circle")
        .data(/* TO FINISH */)
        .enter()
        .append(/* TO FINISH */)
        .attr("r", 20)
        .style("fill", /* TO FINISH */)
        .call(force.drag);


    var svg_texts = svg.selectAll("text")
        .data(nodes)
        .enter()
        .append("text")
        .style("fill", "black")
        .attr("dx", 20)
        .attr("dy", 8)
        .text(/* TO FINISH */);


    force.on("tick", function () {
        svg_edges.attr("x1", /* TO FINISH */)
            .attr("y1", /* TO FINISH */)
            .attr("x2", /* TO FINISH */)
            .attr("y2", /* TO FINISH */);

        svg_nodes.attr("cx", function (d) {
            return d.x;
        })
            .attr("cy", function (d) {
                return d.y;
            });

        svg_texts.attr("x", function (d) {
            return d.x;
        })
            .attr("y", function (d) {
                return d.y;
            });
    });
}
