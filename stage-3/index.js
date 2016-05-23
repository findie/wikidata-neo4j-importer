'use strict';
const clc = require('cli-color');
const async = require('async');

const pad = require('../helper').pad;

/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function(Error)} callback
 */
const stage3 = function(neo4j, lineReader, callback) {
    const session = neo4j.session();

    const linkQuantityes = function _linkQuantities(callback) {
        const timeKey = clc.yellow(pad('Linkied :Quantity to :Item', 70, true));
        console.time(timeKey);
        console.log('Started linking :Quantity to :Item');
        session.run(`
                MATCH (q:Quantity)
                    WHERE q.unit STARTS WITH 'http'
                WITH
                    q,
                    TRIM(SPLIT(q.unit,'/')[-1]) AS itemId

                MATCH (e:Entity) WHERE e.id = itemId

                MERGE (q)-[:UNIT_TYPE]->(e)
                DELETE q.unit
            `)
            .then(() => {
                console.timeEnd(timeKey);
                callback()
            })
            .catch(callback);
    };

    const linkGeoCoordinates = function _linkGeoCoordonates(callback){
        const timeKey = clc.yellow(pad('Linkied :GlobeCoordinate to :Item', 70, true));
        console.time(timeKey);
        console.log('Started linking :GlobeCoordinate to :Item');
        session.run(`
                MATCH (q:GlobeCoordinate)
                    WHERE q.globe STARTS WITH 'http'
                WITH
                    q,
                    TRIM(SPLIT(q.globe,'/')[-1]) AS itemId

                MATCH (e:Entity) WHERE e.id = itemId

                MERGE (q)-[:GLOBE_TYPE]->(e)
                DELETE q.globe
            `)
            .then(() => {
                console.timeEnd(timeKey);
                callback()
            })
            .catch(callback);
    };

    async.series([
        linkQuantityes,
        linkGeoCoordinates
    ], callback);
};

module.exports = stage3;
