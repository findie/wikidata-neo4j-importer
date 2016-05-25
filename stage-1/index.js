'use strict';
const clc = require('cli-color');
const async = require('async');

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

        console.log(clc.yellowBright(`Imported ${lines} lines`));
        console.log(clc.greenBright(
            (((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines)
        ));
        lines += buffer.length;

        function _doQuery(buffer, extraLabel, callback) {
            buffer.forEach(x => delete x.type);
            session
                .run(`
                    UNWIND {buffer} AS item WITH item
                    MERGE (n:${extraLabel}:Entity {id: item.id})
                        ON CREATE SET
                            n = item
                `, { buffer })
                .then(()=>callback())
                .catch(callback)
        }

        async.series([
            _doQuery.bind(null, buffer.filter(x => x.type === entity.type.item), 'Item'),
            _doQuery.bind(null, buffer.filter(x => x.type === entity.type.prop), 'Property')
        ], (err) => {
            callback(err);
        });
    };

    Parallel(_doWork, _done, { concurrency: config.concurrency });
};


module.exports = stage1;