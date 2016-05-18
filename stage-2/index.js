'use strict';
const async = require('async');
const clc = require('cli-color');

const labelify = require('../helper').labelify;
const relationify = require('../helper').relationify;
const distinctify = require('../helper').distinctify;
const pad = require('../helper').pad;

const ETA = require('../helper').ETA;
const Parallel = require('../helper').Parallel;

const config = require('../config.json');
const verbose = config.verbose;

const makeItemBuffer = require('../helper').makeItemBuffer.bind(null, config.bucket);
/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function(Error)} callback
 */
const stage2 = function (neo4j, lineReader, callback) {
    let lines = lineReader.skip;

    console.log('Starting simple node relationship creation...');
    let session = neo4j.session();
    const eta = new ETA(lineReader.total);

    const _done = function (e) {
        session.close();
        callback(e);
    };

    // we first get the props
    // there are about 2.3k so we can store in memory

    const props = {};

    const _getProps = function (cb) {
        session
            .run(`
                MATCH (p:Property) 
                RETURN 
                    p.id AS id,
                    p.label AS label
            `)
            .then(data => {
                data.records.forEach(prop => {
                    props[prop.get('id')] = relationify(prop.get('label'))
                });
                cb();
            })
            .catch(cb);
    }

    const _doWork = function (callback) {
        const buffer = makeItemBuffer(lineReader);

        if (!buffer.length) return callback(null, true);

        console.log(clc.yellowBright(`Imported ${lines} lines`));
        console.log(clc.greenBright(
            (((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines)
        ));
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
                    relation: props[claim.prop] || "CLAIM",
                    prop: claim.prop,
                    id: claim.id
                };

                switch (claim.datatype) {
                    case 'wikibase-property':
                        item.endID = 'P' + claim.value['numeric-id'];
                        return willLinkNodes.push(item);
                    case 'wikibase-item':
                        item.endID = 'Q' + claim.value['numeric-id'];
                        return willLinkNodes.push(item);

                    default:
                        if (!claim.datatype) return;
                        
                        item.label = labelify(claim.datatype);
                        if (claim.value instanceof Object) {
                            item.node = claim.value;
                        } else {
                            item.node = {value: claim.value}
                        }
                        return willGenerateNodes.push(item);
                }
            });

        });

        const link = willLinkNodes.length === 0 ?
            (cb) => cb() :
            (cb) => {

                var distinctRels = distinctify(willLinkNodes, 'relation');

                const runForRelType = function(relType, items, dbcb) {
                    session
                        .run(`
                            UNWIND {items} AS claim
                            WITH claim
                            
                            MATCH (start:Entity), (end:Entity) 
                                WHERE 
                                    start.id = claim.startID AND
                                    end.id = claim.endID
                            WITH start, end, claim
                               
                            MERGE (start)-[:${relType} {by: claim.prop, id: claim.id}]->(end)
                        `, {items})
                        .then(()=>dbcb())
                        .catch(dbcb);
                }

                const timeKey = clc.blue(pad('Linking entities', 70, true));
                console.time(timeKey);
                async.series(
                    Object
                        .keys(distinctRels)
                        .map(relType => {
                            return runForRelType.bind(
                                this,
                                relType,
                                distinctRels[relType]
                            );
                        }),
                    (err) => {
                        console.timeEnd(timeKey);
                        cb(err);
                    }
                )
            };

        const generate = willGenerateNodes.length === 0 ?
            (cb) => cb() :
            (cb) => {

                var distinctRels = distinctify(willGenerateNodes, ['relation', 'label']);

                const runForRelTypeAndLabel = function(relType, label, items, dbcb) {
                    const timeKey = verbose ?
                        clc.red(pad(`-> Generating ${pad(label, 20, true)} and linking ${relType}`, 100, true)) :
                        null;
                    if(verbose) console.time(timeKey);

                    session
                        .run(`
                            UNWIND {items} AS claim 
                            WITH claim
                            
                            MATCH (start:Entity) WHERE start.id = claim.startID
                            MERGE (end:${label}:Claim {id: claim.id}) 
                                SET 
                                    end.id = claim.id,
                                    end = claim.node
                            
                            WITH end, start, claim
                            
                            MERGE (start)-[:${relType} {by: claim.prop}]->(end) 
                        `, {items})
                        .then(()=>{
                            if(verbose) console.timeEnd(timeKey);
                            dbcb();
                        })
                        .catch(dbcb);
                }

                const timeKey = clc.yellow(pad('Generating and linking claims', 70, true));
                console.time(timeKey);
                async.series(
                    Object
                        .keys(distinctRels)
                        .map(key => {
                            return runForRelTypeAndLabel.bind(
                                this,
                                distinctRels[key][0].relation,
                                distinctRels[key][0].label,
                                distinctRels[key]
                            )
                        }),
                    (err) => {
                        console.timeEnd(timeKey);
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

    async.series([
        _getProps
    ], (err) => {
        if (err) return callback(err);

        Parallel(_doWork, _done, {concurrency: config.concurrency});
    });

};

module.exports = stage2;