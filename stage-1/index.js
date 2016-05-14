'use strict';
const entity = require('../helper').entity;
const ETA = require('../helper').ETA;
const Parallel = require('../helper').Parallel;

const config = require('../config.json');

const makeItemBuffer = require('../helper').makeItemBuffer.bind(null, config.bucket);

/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function(Error)} callback
 */
const stage1 = function(neo4j, lineReader, callback) {

    let lines = lineReader.skip;

    console.log('Starting node creation...');
    let session = neo4j.session();
    const eta = new ETA(lineReader.total);

    const _done = function(e) {
        session.close();
        callback(e);
    };

    const _doWork = function(callback) {
        const buffer = makeItemBuffer(lineReader).map(entity.extractNodeData);

        if (!buffer.length) return callback(null, true);

        console.log('Imported', lines, 'lines');
        console.log((((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines));
        lines += buffer.length;

        session
            .run(`
                    UNWIND {buffer} AS item WITH item
                    WHERE item.type = {item}
                    WITH item
                    MERGE (n:Item:Entity {id: item.id})
                        ON CREATE SET 
                            n = item
                    RETURN null
                    
                UNION
                
                    UNWIND {buffer} AS item WITH item
                    WHERE item.type = {prop}
                    WITH item
                    MERGE (n:Property:Entity {id: item.id})
                        ON CREATE SET 
                            n = item
                    RETURN null

            `, { buffer, item: entity.type.item, prop: entity.type.prop })
            .then(()=>callback())
            .catch((e)=>callback(e));
    };

    Parallel(_doWork, _done, {concurrency: config.concurrency});
};


module.exports = stage1;