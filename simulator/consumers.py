import json

from channels.generic.websocket import AsyncWebsocketConsumer

from . import runner


class SimulatorConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("simulator", self.channel_name)
        await self.accept()
        await self.send(
            json.dumps(
                {
                    "type": "status",
                    "data": {"running": runner.is_running()},
                }
            )
        )

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("simulator", self.channel_name)

    async def simulator_event(self, event):
        await self.send(
            json.dumps(
                {
                    "type": event["event_type"],
                    "data": event["data"],
                }
            )
        )
