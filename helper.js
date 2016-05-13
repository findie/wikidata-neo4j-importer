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
    this.lastEtas.push(eta);
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

    while (text.length && !text[text.length - 1])text.pop();


    var term = ['ms', 's', 'm', 'h', 'd'];

    return text.map((x, index) => `${x}${term[index]}`).reverse().join(' ');
};

module.exports.ETA = ETA;