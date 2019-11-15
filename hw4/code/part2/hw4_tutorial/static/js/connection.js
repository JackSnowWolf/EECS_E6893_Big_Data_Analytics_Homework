function connection(nodes, edges) {
    var width = 1440;
    var height = 1080;


    var svg = d3.select("body")
        .append("svg")
        /* TO FINISH */
        .attr("height", height)
        /* TO FINISH */
        .attr("width", width);

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
    /* TO FINISH */
        .data(edges)
        .enter()
        /* TO FINISH */
        .append("line")
        .style("stroke", "#ccc")
        .style("stroke-width", 1);

    var color = d3.scale.category20();


    var svg_nodes = svg.selectAll("circle")
    /* TO FINISH */
        .data(nodes)
        .enter()
        /* TO FINISH */
        .append("circle")
        .attr("r", 20)
        /* TO FINISH */
        .style("fill", function (d) {
            return color(d.index);
        })
        .call(force.drag);


    var svg_texts = svg.selectAll("text")
        .data(nodes)
        .enter()
        .append("text")
        .style("fill", "black")
        .attr("dx", 20)
        .attr("dy", 8)
        /* TO FINISH */
        .text(function (d) {
            return d.node;
        });


    force.on("tick", function () {
        /* TO FINISH */
        svg_edges.attr("x1", function (d) {
            return d.source.x;
        })
        /* TO FINISH */
            .attr("y1", function (d) {
                return d.source.y;
            })
            /* TO FINISH */
            .attr("x2", function (d) {
                return d.target.x;
            })
            /* TO FINISH */
            .attr("y2", function (d) {
                return d.target.y;
            });

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
