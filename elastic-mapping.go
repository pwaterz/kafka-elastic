package main

var mapping = `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"timestamp-nano":{
				"type":"long"
			},
			"timestamp-pst":{
				"type":"text"
			},
			"index-timestamp":{
				"type":"long"
			},
			"partition":{
				"type":"integer"
			},
			"offset":{
				"type":"long"
			},
			"value-raw":{
				"type":"text"
			},
			"value": {
				"dynamic": true,
        "properties": {}
			}
		}
	}
}`
