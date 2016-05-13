const path = require('path');

const neo4j = require('neo4j-driver').v1;
const async = require('async');
const LineByLine = require('n-readlines');

const config = require('./config.json');
const driver = neo4j.driver(config.neo4j.bolt, neo4j.auth.basic(config.neo4j.auth.user, config.neo4j.auth.pass));

const lineReader = new LineByLine(path.resolve(config.file));

const stage0 = require('./stage-0');
const stage1 = require('./stage-1');

async.series([
    (cb) => stage0(driver, cb),
    (cb) => stage1(driver, lineReader, cb)
], (err) => {
    'use strict';
    console.log(err || 'done');
})

