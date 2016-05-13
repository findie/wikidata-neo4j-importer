'use strict';

const helper = {};

helper.type = {
    item: 'item',
    prop: 'property'
};

helper.extractNodeData = (json) => {
    switch (json.type) {
        case helper.type.item:
            return helper.extractNodeDataFromItem(json);
        case helper.type.prop:
            return helper.extractNodeDataFromProp(json);
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

helper.extractNodeDataFromItem = (item) => {
    const obj = {
        id: item.id,
        type: item.type
    };

    Object.assign(obj, extractStaticData(item));

    return obj;
};

helper.extractNodeDataFromProp = (prop) => {
    const obj = {
        id: prop.id,
        datatype: prop.datatype,
        type: prop.type
    };

    Object.assign(obj, extractStaticData(prop));

    return obj;
};

module.exports = helper;