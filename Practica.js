d3.json('Practica/practica_airbnb.json')
    .then((featureCollection) => {
        drawMapAndChart(featureCollection)
    });


function drawMapAndChart(featureCollection) {

    var width = 900;
    var height = 500;

    var svg1 = d3.select('#map')
                .append('svg')
                .attr('width', width)
                .attr('height', height)
                .append('g');

    var center = d3.geoCentroid(featureCollection);

    var projection = d3.geoMercator()
                        .center(center)
                        .fitSize([width, height], featureCollection)
                        .translate([width / 2, height / 2]);
                        
    var pathProjection = d3.geoPath().projection(projection);
    
    var features = featureCollection.features;

    var createPath =  svg1.selectAll('path')
                        .data(features)
                        .enter()
                        .append('path')
                        .attr('d', (d) => pathProjection(d))
                        .attr('fill', 'black')
                        .attr("opacity", function(d, i) {
                            d.opacity = 1
                            return d.opacity
                        });
    
    
    var avgPrice = featureCollection.features.map(function(d){
        
        avgPrice =+ d.properties.avgprice
        avgPrice = +avgPrice || 0
        return avgPrice
    });


    // From continuous values outcomes a discrete output
    var quantileScale  = d3.scaleQuantile()
                        .domain(avgPrice)
                        .range(['green', 'lightgreen', 'yellow', 'orange', 'red']);
    
    
        createPath.attr('fill', (d) => quantileScale(+d.properties.avgprice || 0));

    
    // Legend
    var nblegend = 5;
    var widthRect = (width / nblegend) - 2;
    var heightRect = 10;
    
    
    var legend = svg1.append("g")
            .selectAll("rect")
            .data(quantileScale.range())
            .enter()
            .append("rect")
            .attr("width", widthRect)
            .attr("height", heightRect)
            .attr("x", (d, i) => i * (widthRect + 2))
            .attr("fill", (d) => d);
    
    
    var quantiles =  quantileScale.quantiles()

    quantiles = quantiles.map(function(d){
        return Number(d.toFixed(2));
    });    
    
    var text_legend = svg1.append("g")
            .selectAll("text")
            .data(quantiles)
            .enter()
            .append("text")
            .attr("x", (d, i) => i * (widthRect + 2))
            .attr("y", heightRect * 2.5)
            .text((d) => d)
            .attr("font-size", 12)


    // Create another svg to plot one more chart
    
    var svg2 = d3.select('#chart')
                .append('svg')
                .attr('width', width)
                .attr('height', height)
                .append('g')
                .attr('transform', 'translate(50, 0)')

    length = features[0].properties.avgbedrooms.length

    var bedrooms = features[0].properties.avgbedrooms.map(function(d) {
        return d.bedrooms  
    })
    
    // Scales

    var scaleX = d3.scaleBand()
    .domain(bedrooms)
    .range([0, width])
    .padding(0.3)
    
    var totales = features[0].properties.avgbedrooms.map(function(d) {
        return d.total;
    })

    var scaleY = d3.scaleLinear()
    .domain([0, d3.max(totales)])
    .range([height - 50, 0]);

    var xAxis = d3.axisBottom(scaleX);
    var yAxis = d3.axisLeft(scaleY);

    // Show axis

    svg2.append('g')
        .attr('transform', "translate(0, "+ (height - 50) +")")
        .call(xAxis);
    svg2.append('g')
        .call(yAxis);
    
    // Create chart 

    var rect = svg2.append('g')
                    .selectAll('rect')
                    .data(features[0].properties.avgbedrooms)
                    .enter()
                    .append('rect')
                    .attr('x', function(d){ return scaleX(d.bedrooms) })
                    .attr('y', function(d){ return scaleY(d.total) })
                    .attr('width', scaleX.bandwidth())
                    .attr('height', function(d){ return height - 50 - scaleY(d.total)})
                    .attr('fill', 'black')

    // Create labels
    
    var text =  svg2.append('g')
                    .selectAll('text')
                    .data(features[0].properties.avgbedrooms)
                    .enter()
                    .append('text')
                    .text(d => d.total)
                    .attr("x", function(d) {
                        return scaleX(d.bedrooms) + scaleX.bandwidth() / 2 - 10;
                    })
                    .attr('y', d => {
                        return scaleY(d.total) + 12
                    })
                    .attr("visibility", 'visible')
                    .attr('fill', 'white')

}