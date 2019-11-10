// /**
//  * Created by alienlau on 2019/10/23.
//  */

function dashboard(id, fData) {
    // Define the color to change if your mouse move on the bar
    var barColor = '';

    // Choose color for each word:
    function segColor(c) {
        cmap = {ai: "#4753CC", data: "#828499", good: "#73C9FF", movie: "#CC6E47", spark: '#FFD4B3'};
        return /* TO FINISH */;
    }

    // compute total for each state.
    fData.forEach(function (d) {
        d.total =; /* TO FINISH */
    });

    // function to handle histogram.
    function histoGram(fD) {
        var hG = {}, hGDim = {t: 60, r: 0, b: 30, l: 0};
        hGDim.w = 500 - hGDim.l - hGDim.r,
            hGDim.h = 300 - hGDim.t - hGDim.b;

        //create svg for histogram.
        var hGsvg = d3.select(id).append("svg")
            .attr("width", hGDim.w + hGDim.l + hGDim.r)
            .attr("height", hGDim.h + hGDim.t + hGDim.b).append("g")
            .attr("transform", "translate(" + hGDim.l + "," + hGDim.t + ")");

        // create function for x-axis mapping.
        var x = d3.scale.ordinal().rangeRoundBands([0, hGDim.w], 0.1)
            .domain(fD.map(function (d) {
                return d[0];
            }));

        // Add x-axis to the histogram svg.
        hGsvg.append("g").attr("class", "x axis")
            .attr("transform", "translate(0," + hGDim.h + ")")
            .call(d3.svg.axis().scale(x).orient("bottom"));

        // Create function for y-axis map.
        var y = d3.scale.linear().range([hGDim.h, 0])
            .domain([0, d3.max(fD, function (d) {
                return d[1];
            })]);

        // Create bars for histogram to contain rectangles and count labels.
        var bars = hGsvg.selectAll(".bar").data(fD).enter()
            .append("g").attr("class", "bar");

        //create the rectangles.
        bars.append("rect")
            .attr("x", /* TO FINISH */)
            .attr("y", /* TO FINISH */)
            .attr("width", x.rangeBand())
            .attr("height", function (d) {
                return hGDim.h - y(d[1]);
            })
            .attr('fill', barColor)
            .on("mouseover", mouseover)
            .on("mouseout", mouseout);

        //Create the frequency labels ABOVE the rectangles.
        bars.append("text").text(function (d) {
            return d3.format(",")(d[1])
        })
            .attr("x", function (d) {
                return /* TO FINISH */ +x.rangeBand() / 2;
            })
            .attr("y", /* TO FINISH */)
            .attr("text-anchor", "middle");

        function mouseover(d) {  // utility function to be called on mouseover.
            // filter for selected state.
            var st = fData.filter(function (s) {
                    return s.Time == d[0];
                })[0],
                nD = d3.keys(st.count).map(function (s) {
                    return {type: s, count: st.count[s]};
                });

            // call update functions of pie-chart and legend.
            pC.update(nD);
            leg.update(nD);
        }

        function mouseout(d) {    // utility function to be called on mouseout.
            // reset the pie-chart and legend.
            pC.update(tF);
            leg.update(tF);
        }

        // create function to update the bars. This will be used by pie-chart.
        hG.update = function (nD, color) {
            // update the domain of the y-axis map to reflect change in frequencies.
            y.domain([0, d3.max(nD, function (d) {
                return d[1];
            })]);

            // Attach the new data to the bars.
            var bars = hGsvg.selectAll(".bar").data(nD);

            // transition the height and color of rectangles.
            bars.select("rect").transition().duration(500)
                .attr("y", /* TO FINISH */)
                .attr("height", function (d) {
                    return hGDim.h - y(d[1]);
                })
                .attr("fill", color);

            // transition the frequency labels location and change value.
            bars.select("text").transition().duration(500)
                .text(function (d) {
                    return d3.format(",")(d[1])
                })
                .attr("y", function (d) {
                    return y(d[1]) - 5;
                });
        }
        return hG;
    }

    // function to handle pieChart.
    function pieChart(pD) {
        var pC = {}, pieDim = {w: 250, h: 250};
        pieDim.r = Math.min(pieDim.w, pieDim.h) / 2;

        // create svg for pie chart.
        var piesvg = d3.select(id).append("svg")
            .attr("width", pieDim.w).attr("height", pieDim.h).append("g")
            .attr("transform", "translate(" + pieDim.w / 2 + "," + pieDim.h / 2 + ")");

        // create function to draw the arcs of the pie slices.
        var arc = d3.svg.arc().outerRadius(pieDim.r - 10).innerRadius(0);

        // create a function to compute the pie slice angles.
        var pie = d3.layout.pie().sort(null).value(function (d) {
            return d.count;
        });

        // Draw the pie slices.
        piesvg.selectAll("path").data(pie(pD)).enter().append("path").attr("d", arc)
            .each(function (d) {
                this._current = d;
            })
            .style("fill", function (d) {
                return segColor(d.data.type);
            })
            .on("mouseover", mouseover).on("mouseout", mouseout);

        // create function to update pie-chart. This will be used by histogram.
        pC.update = function (nD) {
            piesvg.selectAll("path").data(pie(nD)).transition().duration(500)
                .attrTween("d", arcTween);
        }

        // Utility function to be called on mouseover a pie slice.
        function mouseover(d) {
            // call the update function of histogram with new data.
            hG.update(fData.map(function (v) {
                return [v.Time, v.count[d.data.type]];
            }), segColor(d.data.type));
        }

        //Utility function to be called on mouseout a pie slice.
        function mouseout(d) {
            // call the update function of histogram with all data.
            hG.update(fData.map(function (v) {
                return [v.Time, v.total];
            }), barColor);
        }

        // Animating the pie-slice requiring a custom function which specifies
        // how the intermediate paths should be drawn.
        function arcTween(a) {
            var i = d3.interpolate(this._current, a);
            this._current = i(0);
            return function (t) {
                return arc(i(t));
            };
        }

        return pC;
    }

    // function to handle legend.
    function legend(lD) {
        var leg = {};

        // create table for legend.
        var legend = d3.select(id).append("table").attr('class', 'legend');

        // create one row per segment.
        var tr = legend.append("tbody").selectAll("tr").data(lD).enter().append("tr");

        // create the first column for each segment.
        tr.append("td").append("svg").attr("width", '16').attr("height", '16').append("rect")
            .attr("width", '16').attr("height", '16')
            .attr("fill", function (d) {
                return segColor(d.type);
            });

        // create the second column for each segment.
        tr.append("td").text(function (d) {
            return d.type;
        });

        // create the third column for each segment.
        tr.append("td").attr("class", 'legendFreq')
            .text(function (d) {
                return d3.format(",")(d.count);
            });

        // create the fourth column for each segment.
        tr.append("td").attr("class", 'legendPerc')
            .text(function (d) {
                return getLegend(d, lD);
            });

        // Utility function to be used to update the legend.
        leg.update = function (nD) {
            // update the data attached to the row elements.
            var l = legend.select("tbody").selectAll("tr").data(nD);

            // update the frequencies.
            l.select(".legendFreq").text(function (d) {
                return d3.format(",")(d.count);
            });

            // update the percentage column.
            l.select(".legendPerc").text(function (d) {
                return getLegend(d, nD);
            });
        }

        function getLegend(d, aD) { // Utility function to compute percentage.
            return d3.format("%")(d.count / d3.sum(aD.map(function (v) {
                return v.count;
            })));
        }

        return leg;
    }

    // calculate total count by segment for all state.
    var tF = ['ai', 'data', 'good', 'movie', 'spark'].map(function (d) {
        return {
            type: d, count: d3.sum(fData.map(function (t) {
                return /* TO FINISH */;
            }))
        };
    });

    // calculate total count by state for all segment.
    var sF = fData.map(function (d) {
        return [d.Time, d.total];
    });

    var hG = histoGram(sF), // create the histogram.
        pC = pieChart(tF), // create the pie-chart.
        leg = legend(tF);  // create the legend.

}