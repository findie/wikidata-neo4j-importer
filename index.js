'use strict';
const path = require('path');
const execSync = require('child_process').execSync;

const neo4j = require('neo4j-driver').v1;
const async = require('async');
const LineByLine = require('n-readlines');

const config = require('./config.json');
const driver = neo4j.driver(config.neo4j.bolt, neo4j.auth.basic(config.neo4j.auth.user, config.neo4j.auth.pass));

console.log('Counting lines');
let lines = config.lines;
if (!lines) lines = execSync(`wc -l ${path.resolve(config.file)}`).toString().trim().split(' ')[0];

console.log('Found', lines, 'lines');

const lineReaderSetup = (lineReader) => {
    lineReader.total = lines;
    lineReader.skip = config.skip || 0;
    
    if (!config.skip) return lineReader;
    for (let i = 0; i < config.skip; i++) {
        if(i % config.bucket == 0) console.log('Skipping', i, '(', i / config.skip * 100, '%)');
        lineReader.next();
    }
    return lineReader;
};

const makeLineReader = () => lineReaderSetup(new LineByLine(path.resolve(config.file), {readChunk: 1024 * 1024 * 64}));

// db cleanup
const stage0 = require('./stage-0');
// node importer
const stage1 = require('./stage-1');
// simple node relations
const stage2 = require('./stage-2');

async.series([
    (cb) => !config.do[0] ? cb() : stage0(driver, cb),
    (cb) => !config.do[1] ? cb() : stage1(driver, makeLineReader(), cb),
    (cb) => !config.do[2] ? cb() : stage2(driver, makeLineReader(), cb)
], (err) => {
    'use strict';
    console.log(err || 'done');
    console.log('exiting in 5 sec');
    setTimeout(process.exit.bind(process, (err ? 1 : 0)), 5000)
});

rocess.on('uncaughtException', function (err) {
    console.err(err.stack || err);
});