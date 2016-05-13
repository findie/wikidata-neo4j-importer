'use strict';

function ETA(full) {
    this.full = full;
    this.lastCurrent = 0;
    this.start = Date.now();
}

ETA.prototype.tick = function(current) {
    const now = Date.now();
    const diff = now - this.start;
    this.start = now;

    const remaining = this.full - current;
    const change = (current - this.lastCurrent);
    this.lastCurrent = change;
    
    return diff * (remaining / change);
};
ETA.prototype.pretty = function(current) {
    let ms = this.tick(current) | 0;

    let text = [];
    text.push(ms % 1000); ms = (ms / 1000) | 0;
    text.push(ms % 60); ms = (ms / 60) | 0;
    text.push(ms % 60); ms = (ms / 60) | 0;
    text.push(ms % 24); ms = (ms / 24) | 0;

    text = text.filter(x => !!x);


    var term = ['ms', 's', 'm', 'h', 'd'];

    return text.map((x, index) => `${x}${term[index]}`).reverse().join(' ');
};

module.exports.ETA = ETA;