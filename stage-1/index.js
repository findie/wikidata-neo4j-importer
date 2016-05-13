'use strict';
const helper = require('./helpers');

const makeItemBuffer = (lineReader) => {

    let line;
    let lines = 10;

    const itemBuffer = [];

    while (itemBuffer.length < lines && (line = lineReader.next())) {
        line = line
            .toString()
            .trim()
            .slice(0, -1);


        // if it's the start or end of the the 72 GB of array, we ignore it
        if (line.length < 2) continue;

        const json = JSON.parse(line);
        const item = helper.extractNodeData(json);

        itemBuffer.push(item);
    }

    return itemBuffer;
};

/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function()} callback
 */
const stage1 = function(neo4j, lineReader, callback) {

    console.log('Starting node creation...');
    var session = neo4j.session();

    function _done(e) {
        session.close();
        callback(e);
    };

    function _doWork() {

        console.log('Requesting item buffer');
        const buffer = makeItemBuffer(lineReader);

        if (!buffer.length) return _done();
        console.log('Writing item buffer of', buffer.length);

        session
            .run(`
                    UNWIND {buffer} AS item WITH item
                    WHERE item.type = {item}
                    WITH item
                    CREATE (n:Item)
                        SET 
                            n = item
                    RETURN null
                    
                UNION
                
                    UNWIND {buffer} AS item WITH item
                    WHERE item.type = {prop}
                    WITH item
                    CREATE (n:Property)
                        SET 
                            n = item
                    RETURN null

            `, { buffer, item: helper.type.item, prop: helper.type.prop })
            .then(_doWork)
            .catch(_done);

    };
    _doWork();
};


module.exports = stage1;