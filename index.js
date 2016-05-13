'use strict';
const path = require('path');
const execSync = require('child_process').execSync;

const neo4j = require('neo4j-driver').v1;
const async = require('async');
const LineByLine = require('n-readlines');

const config = require('./config.json');
const driver = neo4j.driver(config.neo4j.bolt, neo4j.auth.basic(config.neo4j.auth.user, config.neo4j.auth.pass));

console.log('Counting lines');
const lines = execSync(`wc -l ${path.resolve(config.file)}`).toString().trim().split(' ')[0];
console.log('Found'. lines, 'lines');

const lineReaderSetup = (lineReader) => {
    lineReader.total = lines;

    if(!config.skip) return lineReader;
    for(let i = 0; i < config.skip; i ++){
        lineReader.next();
    }
    return lineReader;
};

const lineReader = lineReaderSetup(new LineByLine(path.resolve(config.file)));

const stage0 = require('./stage-0');
const stage1 = require('./stage-1');

async.series([
    (cb) => !config.do[0] ? cb() : stage0(driver, cb),
    (cb) => !config.do[1] ? cb() : stage1(driver, lineReader, cb)
], (err) => {
    'use strict';
    console.log(err || 'done');
    console.log('exiting in 5 sec');
    setTimeout(process.exit.bind(process, (err ? 1 : 0)), 5000)
});

