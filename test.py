import main
import json
#main.reload('','')

main.handler(json.loads("""{
  "messages": [
    {
      "event_metadata": {
        "event_id": "bb1dd06d-a82c-49b4-af98-d8e0c5a1d8f0",
        "event_type": "yandex.cloud.events.storage.ObjectDelete",
        "created_at": "2019-12-19T14:17:47.847365Z",
        "tracing_context": {
          "trace_id": "dd52ace79c62892f",
          "span_id": "",
          "parent_span_id": ""
        },
        "cloud_id": "b1gvlrnlei4l5idm9cbj",
        "folder_id": "b1g88tflru0ek1omtsu0"
      },
      "details": {
        "bucket_id": "archbilling",
        "object_id": "yc-billing-export-with-resources/20201126.csv"
      }
    }
  ]
}
"""), '')


# handler(json.loads("""{
#   "messages": [
#     {
#       "event_metadata": {
#         "event_id": "bb1dd06d-a82c-49b4-af98-d8e0c5a1d8f0",
#         "event_type": "yandex.cloud.events.storage.ObjectDelete",
#         "created_at": "2019-12-19T14:17:47.847365Z",
#         "tracing_context": {
#           "trace_id": "dd52ace79c62892f",
#           "span_id": "",
#           "parent_span_id": ""
#         },
#         "cloud_id": "b1gvlrnlei4l5idm9cbj",
#         "folder_id": "b1g88tflru0ek1omtsu0"
#       },
#       "details": {
# "bucket_id": "vsgrab-dev",
#         "object_id": "test.csv"
#       }
#     }
#   ]
# }
# """), '')