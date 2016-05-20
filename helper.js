'use strict';

const slugify = require('slugify');
const clc = require('cli-color');

function ETA(full) {
    this.full = full;
    this.lastCurrent = 0;
    this.start = Date.now();
    this.lastDiffs = [];
}

ETA.prototype.tick = function (current) {
    const now = Date.now();
    let diff = now - this.start;
    this.start = now;

    if (diff) this.lastDiffs.push(diff);
    if (this.lastDiffs.length > 100) this.lastDiffs.shift();

    diff = this.lastDiffs.reduce((a, c) => a + c, 0) / this.lastDiffs.length;

    const remaining = this.full - current;
    const change = (current - this.lastCurrent);
    this.lastCurrent = current;

    const eta = diff * (remaining / change);

    return eta;
};
ETA.prototype.pretty = function (current) {
    let ms = this.tick(current) | 0;

    let text = [];
    text.push(ms % 1000);
    ms = (ms / 1000) | 0;
    text.push(ms % 60);
    ms = (ms / 60) | 0;
    text.push(ms % 60);
    ms = (ms / 60) | 0;
    text.push(ms % 24);
    ms = (ms / 24) | 0;

    while (text.length && !text[text.length - 1]) text.pop();


    var term = ['ms', 's', 'm', 'h', 'd'];

    return text.map((x, index) => `${x}${term[index]}`).reverse().join(' ');
};

module.exports.ETA = ETA;

function Parallel(work, done, options) {
    options = options || {};
    options.concurrency = options.concurrency || 4;

    const finished = {};

    let _done = function (e, identifier) {
        if (e) {
            _done = _doWork = () => null;
            return done(e);
        }

        finished[identifier] = true;

        if (
            Object
                .keys(finished)
                .map(k=>finished[k])
                .filter(finished=>!finished)
                .length == 0
        ) {
            return done();
        }
    };

    let _doWork = function (identifier) {
        work((err, finish) => {
            if (err) return _done(err);

            if (finish) return _done(null, identifier);

            setImmediate(()=>_doWork(identifier));
        }, identifier);
    };

    for (var i = 0; i < options.concurrency; i++) {
        finished[i] = false;

        setTimeout(_doWork.bind(this, i), (Math.random() + i) * 500);
    }
}

module.exports.Parallel = Parallel;

const makeItemBuffer = (bucket, lineReader) => {

    let line;

    const itemBuffer = [];

    while (itemBuffer.length < bucket && (line = lineReader.next())) {
        line = line
            .toString()
            .trim();

        if (line[line.length - 1] === ',') line = line.slice(0, -1);


        // if it's the start or end of the the 72 GB of array, we ignore it
        if (line.length < 2) continue;

        const json = JSON.parse(line);

        itemBuffer.push(json);
    }

    return itemBuffer;
};

module.exports.makeItemBuffer = makeItemBuffer;

const firstUpper = (str) => str[0].toUpperCase() + str.substr(1);

module.exports.firstUpper = firstUpper;

const labelify = (str) => {
    return str
        .split(/\ |\-|\_|\:/)
        .filter(x=>!!x)
        .map(x=>firstUpper(x.toLowerCase()))
        .join('')
};

module.exports.labelify = labelify;

const _slugify = (str, delim) => {
    delim = delim || '-';
    return slugify(str, delim)
        .replace(new RegExp(`([^a-z0-9\\${delim}]+)`, 'gi'), '');
};

module.exports.slugify = _slugify;

const relationify = (str, delim) => {
    delim = delim || '_';
    const relation = module.exports.slugify(str, delim).toUpperCase();
    
    if (!isNaN(parseInt(relation[0]))) {
        return '_' + relation
    }
    return relation;
};

module.exports.relationify = relationify;


const entity = {};

entity.type = {
    item: 'item',
    prop: 'property'
};

entity.extractNodeData = (json) => {
    switch (json.type) {
        case entity.type.item:
            return entity.extractNodeDataFromItem(json);
        case entity.type.prop:
            return entity.extractNodeDataFromProp(json);
        default:
            throw `Invalid item type ${json.type}`;
    }
};

const extractStaticData = (item) => {
    const obj = {};

    if (item.labels && item.labels.en) {
        if (Array.isArray(item.labels.en)) {
            obj.labels = item.labels.en.map(l => (l.value || '').toString()).filter(x=>!!x)
        } else if (item.labels.en.value) {
            obj.labels = [(item.labels.en.value || '').toString()].filter(x=>!!x)
        }
        obj.label = obj.labels[0];
        delete obj.labels;
    } else {
        obj.label = '';
    }
    obj.slug = module.exports.slugify(obj.label).toLowerCase();

    if (item.descriptions && item.descriptions.en) {
        if (Array.isArray(item.descriptions.en)) {
            obj.descriptions = item.descriptions.en.map(l => (l.value || '').toString()).filter(x=>!!x)
        } else if (item.descriptions.en.value) {
            obj.descriptions = [(item.descriptions.en.value || '').toString()].filter(x=>!!x)
        }
    } else {
        obj.descriptions = [];
    }

    if (item.aliases && item.aliases.en) {
        if (Array.isArray(item.aliases.en)) {
            obj.aliases = item.aliases.en.map(l => (l.value || '').toString()).filter(x=>!!x)
        } else if (item.aliases.en.value) {
            obj.aliases = [(item.aliases.en.value || '').toString()].filter(x=>!!x)
        }
    } else {
        obj.aliases = [];
    }

    return obj;
};

entity.extractNodeDataFromItem = (item) => {
    const obj = {
        id: item.id,
        type: item.type
    };

    Object.assign(obj, extractStaticData(item));

    return obj;
};

entity.extractNodeDataFromProp = (prop) => {
    const obj = {
        id: prop.id,
        datatype: prop.datatype,
        type: prop.type
    };

    Object.assign(obj, extractStaticData(prop));

    return obj;
};

module.exports.entity = entity;

const distinctify = function (items, propKeys, nest) {
    if (!Array.isArray(propKeys)) propKeys = [propKeys];

    const distinct = {};
    items.forEach(item => {
        const masterKey = propKeys
            .map(k => item[k])
            .map(item => item instanceof Object ? JSON.stringify(item) : item)
            .join("%%%%");

        if (!distinct[masterKey]) {
            distinct[masterKey] = [];
        }

        distinct[masterKey].push(item);
    });

    if (!nest) return distinct;

    const obj = {};

    const _ensureObjectForMasterKey = function (maserKey, obj) {
        let pointer = obj;
        const keys = maserKey.split(/%%%%/g);
        keys.forEach(key => {
            if (!pointer[key]) pointer[key] = {};
            pointer = pointer[key];
        });
        return pointer;
    }

    Object.keys(distinct)
        .map(key => {
            const pointer = _ensureObjectForMasterKey(key, obj);

            pointer.items = distinct[key];
        });

    return obj;
}

module.exports.distinctify = distinctify;

const pad = function (str, len, right, pad) {
    pad = pad || ' ';
    right = right === true;

    while (str.length < len) {
        if (right) {
            str += pad;
        } else {
            str = pad + str;
        }
    }

    return str;
}

module.exports.pad = pad;