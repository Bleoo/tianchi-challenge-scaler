import os
import json

if __name__ == '__main__':
    data = '''
    
   {"id":"function_smoke_test","requestFilePath":"/tmp/data/function_smoke_test/requests","metaFilePath":"/tmp/data/function_smoke_test/metas","startTime":"2023-06-28' '09:46:10","endTime":"not' end 'yet","totalRequestsCount":980,"failedRequestsCount":0,"successRate":1.0,"invocationLatencyInSecs":3040,"invocationExecutionTimeInSecs":349,"invocationExecutionTimeInGBs":155,"invocationScheduleTimeInSecs":2690,"invocationIdleTimeInSecs":0,"totalSlotTimeInGBs":26476,"totalLiveSlotCount":0,"score":22.49902165815091}
   
    '''
    data = json.loads(data)
    resourceUsageScore = (data["invocationExecutionTimeInGBs"] / data["totalSlotTimeInGBs"]) * 50
    coldStartTimeScore = (data["invocationExecutionTimeInSecs"] / (
                data["invocationExecutionTimeInSecs"] + data["invocationScheduleTimeInSecs"] + data[
            "invocationIdleTimeInSecs"])) * 50
    score = resourceUsageScore + coldStartTimeScore
    print(score)
