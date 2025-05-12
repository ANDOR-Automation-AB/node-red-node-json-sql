// autotable.js

module.exports = function (RED){
    function AutoTableNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        const tableName = config.tableName;

        node.on('input', function (msg){
            let data = msg.payload;
            let name = tableName || msg.Name;
            
            if (!Array.isArray(data) || data.length === 0){
                node.error("msg.json must be a non-empty array");
                return;
            }

            let first = data[0];
            let columns = [];

            for (let key in first){
                let val = first[key];
                let type = "TEXT";

                if (typeof val === "number"){
                    type = Number.isInteger(val) ? "INT" : "FLOAT";
                } else if (typeof val === "boolean"){
                    type = "BOOLEAN";
                }

                columns.push(`\`${key}\` ${type}`);
            }

            let tableSQL = `CREATE TABLE IF NOT EXISTS \`${name}\` (${columns.join(", ")});`;

            let inserts = data.map(row => {
                let keys = Object.keys(row);
                let cols = keys.map(k => `\`${k}\``).join(", ");
                let vals = keys.map(k => {
                    let v = row[k];
                    if (typeof v === "object") {
                        return `'${JSON.stringify(v).replace(/'/g, "''")}'`;
                    } else if (typeof v === "string"){
                        return `'${v.replace(/'/g, "''")}'`;
                    } else if (v === null || v === undefined){
                        return "NULL";
                    } else {
                        return v;
                    }
                }).join(", ");
                return `INSERT INTO \`${name}\` (${cols}) VALUES (${vals});`;
            });
            
            msg.topic = [tableSQL, ...inserts].join(" ");
            node.send(msg);
        });
    }
    RED.nodes.registerType("autotable", AutoTableNode);
}
