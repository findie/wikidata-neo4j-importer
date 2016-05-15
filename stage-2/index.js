'use strict';
const async = require('async');

const labelify = require('../helper').labelify;
const relationify = require('../helper').relationify;

const ETA = require('../helper').ETA;
const Parallel = require('../helper').Parallel;
const firstUpper = require('../helper').firstUpper;

const config = require('../config.json');

const makeItemBuffer = require('../helper').makeItemBuffer.bind(null, config.bucket);
/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function(Error)} callback
 */
const stage2 = function(neo4j, lineReader, callback) {
    let lines = lineReader.skip;

    console.log('Starting simple node relationship creation...');
    let session = neo4j.session();
    const eta = new ETA(lineReader.total);

    const _done = function(e) {
        session.close();
        callback(e);
    };

    const _doWork = function(callback) {
        const buffer = makeItemBuffer(lineReader);

        if (!buffer.length) return callback(null, true);

        console.log('Imported', lines, 'lines');
        console.log((((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines));
        lines += buffer.length;

        const willLinkNodes = [];
        const willGenerateNodes = [];

        buffer.forEach((item) => {
            if (!item.claims) return;

            const claims = Object.keys(item.claims).map(k => item.claims[k]);

            var obj = {
                id: item.id,
                type: item.type,
                claims: []
            };

            claims.forEach(claims => {
                claims.forEach(claim => {
                    var snack = claim.mainsnak;
                    if (snack.snaktype !== 'value') return;
                    if (claim.type != 'statement') return;

                    return obj.claims.push({
                        datatype: snack.datatype,
                        prop: snack.property,
                        value: snack.datavalue.value,
                        id: claim.id
                    });
                });
            });

            // reshape data

            obj.claims.forEach(claim => {
                var item = {
                    startID: obj.id,
                    prop: claim.prop,
                    id: claim.id
                };

                switch (claim.datatype) {
                    case 'wikibase-property':
                        item.relSufix = 'prop';
                        item.endID = 'P' + claim.value['numeric-id'];
                        return willLinkNodes.push(item);
                    case 'wikibase-item':
                        item.relSufix = 'item';
                        item.endID = 'Q' + claim.value['numeric-id'];
                        return willLinkNodes.push(item);

                    default:
                        if(!claim.datatype) return;

                        item.relSufix = relationify(claim.datatype);
                        item.label = labelify(claim.datatype);
                        if (claim.value instanceof Object) {
                            item.node = claim.value;
                        } else {
                            item.node = { value: claim.value }
                        }
                        return willGenerateNodes.push(item);
                }
            });

        });

        const link = willLinkNodes.length === 0 ?
            (cb) => cb() :
            (cb) => {
                session
                    .run(`
                            UNWIND {data} AS claim
                            WITH claim
                                WHERE claim.relSufix = 'item'
                            WITH claim
                            
                            MATCH (start:Entity), (end:Entity) 
                                WHERE 
                                    start.id = claim.startID AND
                                    end.id = claim.endID
                            WITH start, end, claim
                               
                            MERGE (start)-[:CLAIM_ITEM {by: claim.prop}]->(end)
                            RETURN null
                            
                        UNION
                            
                            UNWIND {data} AS claim
                            WITH claim
                                WHERE claim.relSufix = 'prop'
                            WITH claim
                            
                            MATCH (start:Entity), (end:Entity) 
                                WHERE 
                                    start.id = claim.startID AND
                                    end.id = claim.endID
                            WITH start, end, claim
                               
                            MERGE (start)-[:CLAIM_PROPERTY {by: claim.prop}]->(end)
                            RETURN null
                        `, { data: willLinkNodes })
                    .then(() => {
                        cb();
                    })
                    .catch(cb);
            };

        const generate = willGenerateNodes.length === 0 ?
            (cb) => cb() :
            (cb) => {
                const types = {};
                willGenerateNodes.forEach(claim => {
                    if (!types[claim.label]) {
                        types[claim.label] = {
                            label: claim.label,
                            relSufix: claim.relSufix,
                            claims: []
                        }
                    }

                    types[claim.label].claims.push({
                        startID: claim.startID,
                        prop: claim.prop,
                        node: claim.node,
                        id: claim.id
                    });
                });

                async.series(
                    Object
                        .keys(types)
                        .map(k=>types[k])
                        .map(type => {
                            return (cb) => {
                                session
                                    .run(
                                        `
                                        UNWIND {data} AS claim WITH claim
                                        
                                        MATCH (start:Entity) WHERE start.id = claim.startID
                                        
                                        MERGE (end:${type.label}:Claim {id: claim.id}) 
                                            ON CREATE SET 
                                                end.id = claim.id
                                            SET
                                                end = claim.node
                                        
                                        WITH end, start, claim
                                        
                                        MERGE (start)-[:CLAIM_${type.relSufix} {by: claim.prop}]->(end) 
                                        `,
                                        { data: type.claims }
                                    )
                                    .then(()=> {
                                        cb();
                                    })
                                    .catch(cb);
                            }
                        }),
                    (err) => {
                        cb(err);
                    }
                );

            };

        async.series(
            [link, generate],
            (err) => {
                callback(err);
            }
        );
    };

    Parallel(_doWork, _done, { concurrency: config.concurrency });
};

module.exports = stage2;