'use strict';

function ETA(full) {
    this.full = full;
    this.lastCurrent = 0;
    this.start = Date.now();
    this.lastEtas = [];
}

ETA.prototype.tick = function(current) {
    const now = Date.now();
    const diff = now - this.start;
    this.start = now;

    const remaining = this.full - current;
    const change = (current - this.lastCurrent);
    this.lastCurrent = current;

    const eta = diff * (remaining / change);
    if (!isNaN(parseInt(eta))) this.lastEtas.push(eta);
    if (this.lastEtas.length > 10) this.lastEtas.shift();

    return this.lastEtas.reduce((a, c)=> a + c, 0) / this.lastEtas.length;
};
ETA.prototype.pretty = function(current) {
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

    let _done = function(e, identifier) {
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

    let _doWork = function(identifier) {
        work((err, finish) => {
            if (err) return _done(err);

            if (finish) return _done(null, identifier);

            setImmediate(()=>_doWork(identifier));
        });
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
            .trim()
            .slice(0, -1);


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

const relationify = (str) => {
    return str
        .split(/\ |\-|\_|\:/)
        .filter(x=>!!x)
        .map(x=>x.toUpperCase())
        .join('_')
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
    } else {
        obj.labels = [];
    }

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