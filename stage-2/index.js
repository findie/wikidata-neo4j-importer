'use strict';
const async = require('async');
const clc = require('cli-color');

const labelify = require('../helper').labelify;
const relationify = require('../helper').relationify;
const distinctify = require('../helper').distinctify;
const pad = require('../helper').pad;

const ETA = require('../helper').ETA;
const Parallel = require('../helper').Parallel;
const deadLockRetrier = require('../helper').deadLockRetrier;

const config = require('../config.json');

const makeItemBuffer = require('../helper').makeItemBuffer.bind(null, config.bucket);

const linkNodes = function _linkNodes(neo4j, willLinkNodes, identifier, cb) {

  const timeKey = clc.blue(pad(`Linking entities (${identifier})`, 70, true));
  console.time(timeKey);
  const session = neo4j.session();

  deadLockRetrier(
    session,
    `
        UNWIND {items} AS claim
        WITH claim
    
        MATCH 
            (start:Entity {id: claim.startID}),
            (end:Entity {id: claim.endID})
            
        USING INDEX start:Entity(id)
        USING INDEX end:Entity(id)
        
        WITH start, end, claim
    
        CALL apoc.create.relationship(
            start, 
            claim.relation,
            {by: claim.prop, id: claim.id},
            end
        ) YIELD rel
        
        RETURN COUNT(*)
    `,
    { items: willLinkNodes },
    () => {
      console.timeEnd(timeKey);
      session.close();
      return cb();
    },
    err => {
      session.close();
      cb(err)
    }
  );
};

const generateClaims = function _generateClaims(neo4j, willGenerateNodes, identifier, cb) {
  const timeKey = config.verbose ?
    clc.red(pad(`Generating Claims and linking ${willGenerateNodes.length} (${identifier})`, 70, true)) :
    null;

  if (config.verbose) console.time(timeKey);
  const session = neo4j.session();

  deadLockRetrier(
    session,
    `
            UNWIND {items} AS claim
            WITH claim

            MATCH 
                (start:Entity {id: claim.startID})
            USING INDEX start:Entity(id)
            
            MERGE (end:Claim {id: claim.id})
            SET
                end += claim.node

            WITH end, start, claim
 
            CALL apoc.create.addLabels([end], [claim.label]) YIELD node

            WITH end, start, claim

            CALL apoc.create.relationship(
                start,
                claim.relation,
                {by: claim.prop},
                end
            ) YIELD rel
            
            RETURN COUNT(*)
        `,
    { items: willGenerateNodes },
    () => {
      if (config.verbose) console.timeEnd(timeKey);
      session.close();
      cb();
    },
    err => {
      session.close();
      cb(err);
    }
  );
};


/**
 * here we add all the nodes in the DB (item and property)
 * @param {Driver} neo4j
 * @param {LineByLine} lineReader
 * @param {function(Error)} callback
 */
const stage2 = function(neo4j, lineReader, callback) {
  let lines = lineReader.skip;

  console.log('Starting simple node relationship creation...');

  const eta = new ETA(lineReader.total);

  const _done = function(e) {
    return callback(e);
  };

  // we first get the :Property
  // there are about 2.3k so we can store in memory
  const props = {};

  const _getProps = function(cb) {
    const session = neo4j.session();
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
        session.close();
        cb();
      })
      .catch((err) => {
        session.close();
        cb(err)
      });
  };

  const _doWork = function(callback, identifier) {
    const buffer = makeItemBuffer(lineReader);

    if (!buffer.length) return callback(null, true);

    console.log(clc.yellowBright(`Imported ${lines} lines`));
    console.log(clc.greenBright(
      (((lines / lineReader.total) * 100000) | 0) / 1000 + '%', 'done!', ' -> ', 'Remaining', eta.pretty(lines)
    ));
    lines += buffer.length;

    const willLinkNodes = [];
    const willGenerateNodes = [];

    for (let i = 0; i < buffer.length; i++) {
      const item = buffer[i];

      if (!item.claims) continue;

      const claims = Object.keys(item.claims).d_map(k => item.claims[k]);

      const obj = {
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
              item.node = { value: claim.value }
            }
            return willGenerateNodes.push(item);
        }
      });
    }

    const link = willLinkNodes.length === 0 ?
      (cb) => cb() :
      (cb) => linkNodes(neo4j, willLinkNodes, identifier, cb);

    const generate = willGenerateNodes.length === 0 ?
      (cb) => cb() :
      (cb) => generateClaims(neo4j, willGenerateNodes, identifier, cb);

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

    Parallel(_doWork, _done, { concurrency: config.concurrency });
  });

};

module.exports = stage2;