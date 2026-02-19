function buildGraph(data) {

    let elements = [];

    data.nodes.forEach(node => {
        elements.push({
            data: {
                id: node.account_id,
                label: node.account_id,
                score: node.suspicion_score
            }
        });
    });

    data.edges.forEach(edge => {
        elements.push({
            data: {
                source: edge.source,
                target: edge.target
            }
        });
    });

    let cy = cytoscape({
        container: document.getElementById('graph-container'),
        elements: elements,
        style: [
            {
                selector: 'node',
                style: {
                    'label': 'data(label)',
                    'background-color': ele =>
                        ele.data('score') > 0 ? '#ef4444' : '#38bdf8',
                    'border-width': ele =>
                        ele.data('score') > 0 ? 6 : 2,
                    'border-color': '#ffffff',
                    'width': 45,
                    'height': 45,
                    'text-valign': 'center',
                    'color': '#fff'
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 2,
                    'line-color': '#64748b',
                    'target-arrow-shape': 'triangle',
                    'target-arrow-color': '#64748b',
                    'curve-style': 'bezier'
                }
            }
        ],
        layout: {
            name: 'cose',
            idealEdgeLength: 120,
            nodeRepulsion: 800000,
            animate: true
        }
    });

    cy.on('tap', 'node', function(evt) {
        let node = evt.target;

        alert(
            "Account: " + node.id() +
            "\nSuspicion Score: " + node.data('score')
        );
    });
}

buildGraph(backendData);
