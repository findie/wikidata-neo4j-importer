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
  const eta = new ETA(lineReader.total);

  const _done = function(e) {
    callback(e);
  };

  const _doWork = function(callback, identifier) {
    const buffer = makeItemBuffer(lineReader).d_map(entity.extractNodeData);

    if (!buffer.length) return callback(null, true);

    console.log(clc.yellowBright(`Imported ${lines} lines`));
    console.log(clc.greenBright(
      (((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines)
    ));
    lines += buffer.length;

    function _doQuery(buffer, extraLabel, callback) {
      let session = neo4j.session();

      buffer.forEach(x => delete x.type);
      session
        .run(`
          UNWIND {buffer} AS nodeData 
          CALL apoc.create.node([ {extraLabel}, 'Entity' ], {}) YIELD node
          SET node = nodeData
          RETURN COUNT(*)
                `, { buffer, extraLabel })
        .then(_ => {
          session.close();
          callback()
        }, err => {
          session.close();
          callback(err);
        })
    }

    async.series([
      _doQuery.bind(null, buffer.d_filter(x => x.type === entity.type.item), 'Item'),
      _doQuery.bind(null, buffer.d_filter(x => x.type === entity.type.prop), 'Property')
    ], (err) => {
      callback(err);
    });
  };

  Parallel(_doWork, _done, { concurrency: config.concurrency });
};


module.exports = stage1;