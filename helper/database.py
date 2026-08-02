import motor.motor_asyncio
from datetime import datetime, timedelta
import time
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from config import Config

class MongoDB:
    _instances = {}

    def __new__(cls, uri: str, db_name: str):
        if (uri, db_name) not in cls._instances:
            instance = super().__new__(cls)
            instance.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            instance.db = instance.client[db_name]
            instance.user_data = instance.db["users"]
            instance.channel_data = instance.db["channels"]
            instance.premium_users = instance.db['pros']
            instance.fsub_status = instance.db['fsub_status']  # New collection for fsub status tracking
            instance.request_sub = instance.db['request_sub']  # New collection for join request tracking
            cls._instances[(uri, db_name)] = instance
        return cls._instances[(uri, db_name)]

    async def set_channels(self, channels: list[int]):
        await self.user_data.update_one(
            {"_id": 1},
            {"$set": {"channels": channels}},
            upsert=True
        )

    async def get_channels(self) -> list[int]:
        data = await self.user_data.find_one({"_id": 1})
        return data.get("channels", []) if data else []

    async def add_channel_user(self, channel_id: int, user_id: int):
        await self.channel_data.update_one(
            {"_id": channel_id},
            {"$addToSet": {"users": user_id}},
            upsert=True
        )

    async def remove_channel_user(self, channel_id: int, user_id: int):
        await self.channel_data.update_one(
            {"_id": channel_id},
            {"$pull": {"users": user_id}}
        )

    async def get_channel_users(self, channel_id: int) -> list[int]:
        doc = await self.channel_data.find_one({"_id": channel_id})
        return doc.get("users", []) if doc else []

    async def is_user_in_channel(self, channel_id: int, user_id: int) -> bool:
        doc = await self.channel_data.find_one(
            {"_id": channel_id, "users": {"$in": [user_id]}},
            {"_id": 1}
        )
        return doc is not None

    # ✅ PRO FEATURES

    async def add_pro(self, user_id: int, expiry_date: datetime = None):
        try:
            await self.premium_users.update_one(
                {'_id': user_id},
                {'$set': {'expiry_date': expiry_date}},
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Failed to add premium user: {e}")
            return False

    async def remove_pro(self, user_id: int):
        try:
            await self.premium_users.delete_one({'_id': user_id})
            return True
        except Exception as e:
            print(f"Failed to remove premium user: {e}")
            return False

    async def is_pro(self, user_id: int):
        doc = await self.premium_users.find_one({'_id': user_id})
        if not doc:
            return False
        if 'expiry_date' not in doc:
            return True  # Legacy premium users without expiry date
        if doc['expiry_date'] is None:
            return True  # Permanent premium users
        return doc['expiry_date'] > datetime.now()

    async def get_pros_list(self):
        current_time = datetime.now()
        cursor = self.premium_users.find({
            '$or': [
                {'expiry_date': None},  # Permanent premium users
                {'expiry_date': {'$exists': False}},  # Legacy premium users
                {'expiry_date': {'$gt': current_time}}  # Active premium users
            ]
        })
        return [doc['_id'] async for doc in cursor]
        
    async def get_expiry_date(self, user_id: int) -> datetime:
        doc = await self.premium_users.find_one({'_id': user_id})
        return doc.get('expiry_date') if doc else None

    # ✅ USER FUNCTIONS

    async def present_user(self, user_id: int) -> bool:
        found = await self.user_data.find_one({'_id': user_id})
        return bool(found)

    async def add_user(self, user_id: int, ban: bool = False):
        await self.user_data.insert_one({'_id': user_id, 'ban': ban})

    async def full_userbase(self) -> list[int]:
        cursor = self.user_data.find()
        return [doc['_id'] async for doc in cursor]

    async def del_user(self, user_id: int):
        await self.user_data.delete_one({'_id': user_id})

    async def ban_user(self, user_id: int):
        await self.user_data.update_one({'_id': user_id}, {'$set': {'ban': True}})

    async def unban_user(self, user_id: int):
        await self.user_data.update_one({'_id': user_id}, {'$set': {'ban': False}})

    async def is_banned(self, user_id: int) -> bool:
        user = await self.user_data.find_one({'_id': user_id})
        return user.get('ban', False) if user else False

    # ✅ FSUB CHANNELS FUNCTIONS

    async def set_fsub_channels(self, fsub_data: dict):
        """Store fsub channels data to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "fsub_channels"},
            {"$set": {"channels": fsub_data}},
            upsert=True
        )

    async def get_fsub_channels(self) -> dict:
        """Get fsub channels data from database"""
        data = await self.user_data.find_one({"_id": "fsub_channels"})
        return data.get("channels", {}) if data else {}

    async def add_fsub_channel(self, channel_id: int, channel_data: list):
        """Add a single fsub channel to database"""
        current_data = await self.get_fsub_channels()
        current_data[str(channel_id)] = channel_data
        await self.set_fsub_channels(current_data)

    async def remove_fsub_channel(self, channel_id: int):
        """Remove a single fsub channel from database"""
        current_data = await self.get_fsub_channels()
        current_data.pop(str(channel_id), None)
        await self.set_fsub_channels(current_data)

    # ✅ SHORTNER SETTINGS FUNCTIONS

    async def set_shortner_settings(self, shortner_data: dict):
        """Store shortner settings to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "shortner_settings"},
            {"$set": {"settings": shortner_data}},
            upsert=True
        )

    async def get_shortner_settings(self) -> dict:
        """Get shortner settings from database"""
        data = await self.user_data.find_one({"_id": "shortner_settings"})
        return data.get("settings", {}) if data else {}

    async def update_shortner_setting(self, key: str, value: str):
        """Update a single shortner setting"""
        current_data = await self.get_shortner_settings()
        current_data[key] = value
        await self.set_shortner_settings(current_data)

    async def get_shortner_status(self) -> bool:
        """Get shortner on/off status"""
        settings = await self.get_shortner_settings()
        return settings.get('enabled', True)  # Default is enabled

    async def set_shortner_status(self, enabled: bool):
        """Set shortner on/off status"""
        await self.update_shortner_setting('enabled', enabled)

    # ✅ FSUB STATUS COLLECTION FUNCTIONS

    async def update_fsub_status(self, user_id: int, channel_id: int, status: str):
        """Update user's subscription status for a specific channel"""
        await self.fsub_status.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"status": status, "last_updated": datetime.now()}},
            upsert=True
        )

    async def get_fsub_status(self, user_id: int, channel_id: int) -> str:
        """Get user's subscription status for a specific channel"""
        doc = await self.fsub_status.find_one({"user_id": user_id, "channel_id": channel_id})
        return doc.get("status") if doc else None

    async def remove_fsub_status(self, user_id: int, channel_id: int):
        """Remove user's fsub status record"""
        await self.fsub_status.delete_one({"user_id": user_id, "channel_id": channel_id})

    async def get_user_fsub_statuses(self, user_id: int) -> dict:
        """Get all fsub statuses for a user"""
        cursor = self.fsub_status.find({"user_id": user_id})
        statuses = {}
        async for doc in cursor:
            statuses[doc["channel_id"]] = doc["status"]
        return statuses

    async def clear_expired_fsub_statuses(self, days: int = 7):
        """Clear fsub status records older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        await self.fsub_status.delete_many({"last_updated": {"$lt": cutoff_date}})

    # ✅ REQUEST SUB COLLECTION FUNCTIONS

    async def add_join_request(self, user_id: int, channel_id: int, request_id: int = None):
        """Record a join request submission"""
        await self.request_sub.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {
                "request_id": request_id,
                "status": "pending",
                "submitted_at": datetime.now(),
                "last_updated": datetime.now()
            }},
            upsert=True
        )

    async def update_join_request_status(self, user_id: int, channel_id: int, status: str):
        """Update join request status (pending, approved, rejected)"""
        await self.request_sub.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"status": status, "last_updated": datetime.now()}}
        )

    async def get_join_request_status(self, user_id: int, channel_id: int) -> str:
        """Get join request status"""
        doc = await self.request_sub.find_one({"user_id": user_id, "channel_id": channel_id})
        return doc.get("status") if doc else None

    async def has_submitted_join_request(self, user_id: int, channel_id: int) -> bool:
        """Check if user has submitted a join request for channel"""
        doc = await self.request_sub.find_one({"user_id": user_id, "channel_id": channel_id})
        return doc is not None

    async def remove_join_request(self, user_id: int, channel_id: int):
        """Remove join request record"""
        await self.request_sub.delete_one({"user_id": user_id, "channel_id": channel_id})

    async def get_pending_requests_for_channel(self, channel_id: int) -> list:
        """Get all pending join requests for a channel"""
        cursor = self.request_sub.find({"channel_id": channel_id, "status": "pending"})
        requests = []
        async for doc in cursor:
            requests.append({
                "user_id": doc["user_id"],
                "request_id": doc.get("request_id"),
                "submitted_at": doc["submitted_at"]
            })
        return requests

    async def clear_old_join_requests(self, days: int = 30):
        """Clear join request records older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        await self.request_sub.delete_many({"submitted_at": {"$lt": cutoff_date}})

    async def cleanup_database(self):
        """Perform comprehensive database maintenance - clean old records and validate data"""
        try:
            # Clean old fsub status records (older than 7 days)
            await self.clear_expired_fsub_statuses(7)
            # Clean old join request records (older than 30 days)
            await self.clear_old_join_requests(30)
            
            # Additional cleanup for orphaned records
            await self.cleanup_orphaned_records()
            
            return True
        except Exception as e:
            print(f"Database cleanup error: {e}")
            return False

    async def cleanup_orphaned_records(self):
        """Clean up records that are no longer valid"""
        try:
            # Remove fsub status records for users who no longer exist
            users = await self.full_userbase()
            user_ids_set = set(users)
            
            # Clean fsub_status collection
            async for doc in self.fsub_status.find({"user_id": {"$nin": users}}):
                await self.fsub_status.delete_one({"_id": doc["_id"]})
            
            # Clean request_sub collection
            async for doc in self.request_sub.find({"user_id": {"$nin": users}}):
                await self.request_sub.delete_one({"_id": doc["_id"]})
                
            return True
        except Exception as e:
            print(f"Error cleaning orphaned records: {e}")
            return False

    async def get_comprehensive_fsub_statistics(self):
        """Get detailed statistics about fsub system"""
        try:
            # Basic counts
            fsub_count = await self.fsub_status.count_documents({})
            request_count = await self.request_sub.count_documents({})
            pending_requests = await self.request_sub.count_documents({"status": "pending"})
            approved_requests = await self.request_sub.count_documents({"status": "approved"})
            rejected_requests = await self.request_sub.count_documents({"status": "rejected"})
            
            # Status breakdown
            status_breakdown = {}
            async for doc in self.fsub_status.aggregate([
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]):
                status_breakdown[doc["_id"]] = doc["count"]
            
            # Channel-wise statistics
            channel_stats = {}
            async for doc in self.fsub_status.aggregate([
                {"$group": {"_id": "$channel_id", "count": {"$sum": 1}}}
            ]):
                channel_stats[doc["_id"]] = doc["count"]
            
            # Recent activity (last 24 hours)
            from datetime import datetime, timedelta
            yesterday = datetime.now() - timedelta(days=1)
            recent_fsub_updates = await self.fsub_status.count_documents(
                {"last_updated": {"$gte": yesterday}}
            )
            recent_requests = await self.request_sub.count_documents(
                {"submitted_at": {"$gte": yesterday}}
            )
            
            return {
                "total_fsub_records": fsub_count,
                "total_join_requests": request_count,
                "pending_requests": pending_requests,
                "approved_requests": approved_requests,
                "rejected_requests": rejected_requests,
                "status_breakdown": status_breakdown,
                "channel_statistics": channel_stats,
                "recent_activity": {
                    "fsub_updates_24h": recent_fsub_updates,
                    "join_requests_24h": recent_requests
                }
            }
        except Exception as e:
            print(f"Error getting comprehensive fsub statistics: {e}")
            return {}

    async def get_user_activity_summary(self, user_id: int):
        """Get comprehensive activity summary for a specific user"""
        try:
            # Get all fsub statuses
            fsub_statuses = await self.get_user_fsub_statuses(user_id)
            
            # Get join request history
            join_requests = []
            async for doc in self.request_sub.find({"user_id": user_id}):
                join_requests.append({
                    "channel_id": doc["channel_id"],
                    "status": doc["status"],
                    "submitted_at": doc["submitted_at"],
                    "last_updated": doc["last_updated"]
                })
            
            # Check if user is banned
            is_banned = await self.is_banned(user_id)
            
            # Check if user is premium
            is_premium = await self.is_pro(user_id)
            
            return {
                "user_id": user_id,
                "is_banned": is_banned,
                "is_premium": is_premium,
                "fsub_statuses": fsub_statuses,
                "join_request_history": join_requests,
                "total_channels_joined": len([s for s in fsub_statuses.values() if s == "joined"]),
                "total_requests_submitted": len(join_requests)
            }
        except Exception as e:
            print(f"Error getting user activity summary for {user_id}: {e}")
            return {}

    async def get_channel_activity_summary(self, channel_id: int):
        """Get comprehensive activity summary for a specific channel"""
        try:
            # Get all users in channel
            channel_users = await self.get_channel_users(channel_id)
            
            # Get fsub status breakdown for this channel
            status_counts = {}
            async for doc in self.fsub_status.aggregate([
                {"$match": {"channel_id": channel_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]):
                status_counts[doc["_id"]] = doc["count"]
            
            # Get join request stats for this channel
            request_stats = {}
            async for doc in self.request_sub.aggregate([
                {"$match": {"channel_id": channel_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]):
                request_stats[doc["_id"]] = doc["count"]
            
            # Get recent activity (last 7 days)
            from datetime import datetime, timedelta
            week_ago = datetime.now() - timedelta(days=7)
            recent_joins = await self.fsub_status.count_documents({
                "channel_id": channel_id,
                "status": "joined",
                "last_updated": {"$gte": week_ago}
            })
            
            recent_requests = await self.request_sub.count_documents({
                "channel_id": channel_id,
                "submitted_at": {"$gte": week_ago}
            })
            
            return {
                "channel_id": channel_id,
                "total_users": len(channel_users),
                "status_breakdown": status_counts,
                "request_statistics": request_stats,
                "recent_activity_7d": {
                    "new_joins": recent_joins,
                    "new_requests": recent_requests
                }
            }
        except Exception as e:
            print(f"Error getting channel activity summary for {channel_id}: {e}")
            return {}

    async def bulk_update_user_statuses(self, updates: list):
        """Perform bulk updates for user statuses - useful for synchronization"""
        try:
            operations = []
            for update in updates:
                user_id = update["user_id"]
                channel_id = update["channel_id"]
                status = update["status"]
                
                operations.append({
                    "updateOne": {
                        "filter": {"user_id": user_id, "channel_id": channel_id},
                        "update": {
                            "$set": {
                                "status": status,
                                "last_updated": datetime.now()
                            }
                        },
                        "upsert": True
                    }
                })
            
            if operations:
                result = await self.fsub_status.bulk_write(operations)
                return result
            return None
        except Exception as e:
            print(f"Error in bulk update user statuses: {e}")
            return None

    async def sync_channel_members(self, channel_id: int, current_members: list):
        """Synchronize database with actual channel members"""
        try:
            # Get stored users for this channel
            stored_users = await self.get_channel_users(channel_id)
            
            # Find users to add (in channel but not in database)
            users_to_add = set(current_members) - set(stored_users)
            
            # Find users to remove (in database but not in channel)
            users_to_remove = set(stored_users) - set(current_members)
            
            # Add new users
            for user_id in users_to_add:
                await self.add_channel_user(channel_id, user_id)
                await self.update_fsub_status(user_id, channel_id, "joined")
            
            # Remove old users
            for user_id in users_to_remove:
                await self.remove_channel_user(channel_id, user_id)
                await self.update_fsub_status(user_id, channel_id, "left")
            
            return {
                "added": len(users_to_add),
                "removed": len(users_to_remove),
                "synced": True
            }
        except Exception as e:
            print(f"Error syncing channel members for {channel_id}: {e}")
            return {"synced": False, "error": str(e)}

    async def export_fsub_data(self, channel_id: int = None):
        """Export force subscription data for backup or analysis"""
        try:
            export_data = {
                "export_timestamp": datetime.now().isoformat(),
                "fsub_statuses": [],
                "join_requests": []
            }
            
            # Filter by channel if specified
            filter_query = {"channel_id": channel_id} if channel_id else {}
            
            # Export fsub statuses
            async for doc in self.fsub_status.find(filter_query):
                doc["_id"] = str(doc["_id"])  # Convert ObjectId to string
                if "last_updated" in doc:
                    doc["last_updated"] = doc["last_updated"].isoformat()
                export_data["fsub_statuses"].append(doc)
            
            # Export join requests
            async for doc in self.request_sub.find(filter_query):
                doc["_id"] = str(doc["_id"])  # Convert ObjectId to string
                if "submitted_at" in doc:
                    doc["submitted_at"] = doc["submitted_at"].isoformat()
                if "last_updated" in doc:
                    doc["last_updated"] = doc["last_updated"].isoformat()
                export_data["join_requests"].append(doc)
            
            return export_data
        except Exception as e:
            print(f"Error exporting fsub data: {e}")
            return None

    async def get_fsub_statistics(self):
        """Get statistics about fsub collections"""
        try:
            fsub_count = await self.fsub_status.count_documents({})
            request_count = await self.request_sub.count_documents({})
            pending_requests = await self.request_sub.count_documents({"status": "pending"})
            
            return {
                "fsub_status_records": fsub_count,
                "join_request_records": request_count,
                "pending_requests": pending_requests
            }
        except Exception as e:
            print(f"Error getting fsub statistics: {e}")
            return {}

    # ✅ DB CHANNELS FUNCTIONS

    async def set_db_channels(self, db_channels_data: dict):
        """Store DB channels data to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "db_channels"},
            {"$set": {"channels": db_channels_data}},
            upsert=True
        )

    async def get_db_channels(self) -> dict:
        """Get DB channels data from database"""
        data = await self.user_data.find_one({"_id": "db_channels"})
        return data.get("channels", {}) if data else {}

    async def add_db_channel(self, channel_id: int, channel_data: dict):
        """Add a single DB channel to database"""
        current_data = await self.get_db_channels()
        current_data[str(channel_id)] = channel_data
        await self.set_db_channels(current_data)

    async def remove_db_channel(self, channel_id: int):
        """Remove a single DB channel from database"""
        current_data = await self.get_db_channels()
        current_data.pop(str(channel_id), None)
        await self.set_db_channels(current_data)

    async def update_db_channel(self, channel_id: int, channel_data: dict):
        """Update a single DB channel in database"""
        current_data = await self.get_db_channels()
        if str(channel_id) in current_data:
            current_data[str(channel_id)].update(channel_data)
            await self.set_db_channels(current_data)

    async def get_primary_db_channel(self) -> int:
        """Get the primary DB channel ID"""
        db_channels = await self.get_db_channels()
        for channel_id_str, channel_data in db_channels.items():
            if channel_data.get('is_primary', False):
                return int(channel_id_str)
        return None

    async def set_primary_db_channel(self, channel_id: int):
        """Set a DB channel as primary (remove primary from others)"""
        db_channels = await self.get_db_channels()
        # Remove primary status from all channels
        for ch_id, ch_data in db_channels.items():
            ch_data['is_primary'] = False
        # Set new primary channel
        if str(channel_id) in db_channels:
            db_channels[str(channel_id)]['is_primary'] = True
        await self.set_db_channels(db_channels)

    async def get_active_db_channels(self) -> dict:
        """Get all active DB channels"""
        db_channels = await self.get_db_channels()
        active_channels = {}
        for channel_id_str, channel_data in db_channels.items():
            if channel_data.get('is_active', True):
                active_channels[channel_id_str] = channel_data
        return active_channels

    async def toggle_db_channel_status(self, channel_id: int):
        """Toggle DB channel active/inactive status"""
        db_channels = await self.get_db_channels()
        if str(channel_id) in db_channels:
            current_status = db_channels[str(channel_id)].get('is_active', True)
            db_channels[str(channel_id)]['is_active'] = not current_status
            await self.set_db_channels(db_channels)
            return not current_status
        return None


    # ✅ BOT SETTINGS FUNCTIONS

    async def set_bot_settings(self, settings_data: dict):
        """Store bot settings to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "bot_settings"},
            {"$set": {"settings": settings_data}},
            upsert=True
        )

    async def get_bot_settings(self) -> dict:
        """Get bot settings from database"""
        data = await self.user_data.find_one({"_id": "bot_settings"})
        return data.get("settings", {}) if data else {}

    async def update_bot_setting(self, key: str, value):
        """Update a single bot setting"""
        current_data = await self.get_bot_settings()
        current_data[key] = value
        await self.set_bot_settings(current_data)

    async def get_bot_setting(self, key: str, default=None):
        """Get a single bot setting with default fallback"""
        settings = await self.get_bot_settings()
        return settings.get(key, default)

    # ✅ MESSAGES SETTINGS FUNCTIONS

    async def set_messages_settings(self, messages_data: dict):
        """Store messages settings to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "messages_settings"},
            {"$set": {"messages": messages_data}},
            upsert=True
        )

    async def get_messages_settings(self) -> dict:
        """Get messages settings from database"""
        data = await self.user_data.find_one({"_id": "messages_settings"})
        return data.get("messages", {}) if data else {}

    async def update_message_setting(self, key: str, value: str):
        """Update a single message setting"""
        current_data = await self.get_messages_settings()
        current_data[key] = value
        await self.set_messages_settings(current_data)

    async def get_message_setting(self, key: str, default: str = ""):
        """Get a single message setting with default fallback"""
        messages = await self.get_messages_settings()
        return messages.get(key, default)

    # ✅ ADMIN SETTINGS FUNCTIONS

    async def set_admins_list(self, admins_list: list):
        """Store admins list to database for persistence across bot restarts"""
        await self.user_data.update_one(
            {"_id": "admins_list"},
            {"$set": {"admins": admins_list}},
            upsert=True
        )

    async def get_admins_list(self) -> list:
        """Get admins list from database"""
        data = await self.user_data.find_one({"_id": "admins_list"})
        return data.get("admins", []) if data else []

    async def add_admin(self, admin_id: int):
        """Add an admin to the database"""
        current_admins = await self.get_admins_list()
        if admin_id not in current_admins:
            current_admins.append(admin_id)
            await self.set_admins_list(current_admins)
            return True
        return False

    async def remove_admin(self, admin_id: int):
        """Remove an admin from the database"""
        current_admins = await self.get_admins_list()
        if admin_id in current_admins:
            current_admins.remove(admin_id)
            await self.set_admins_list(current_admins)
            return True
        return False

    # ✅ BATCH SETTINGS FUNCTIONS

    async def save_all_settings(self, bot_settings: dict, messages: dict, admins: list):
        """Save all settings in a single transaction for efficiency"""
        try:
            await self.set_bot_settings(bot_settings)
            await self.set_messages_settings(messages)
            await self.set_admins_list(admins)
            return True
        except Exception as e:
            print(f"Error saving all settings: {e}")
            return False

    async def load_all_settings(self) -> dict:
        """Load all settings in a single call for efficiency"""
        try:
            bot_settings = await self.get_bot_settings()
            messages = await self.get_messages_settings()
            admins = await self.get_admins_list()
            shortner_settings = await self.get_shortner_settings()
            
            return {
                "bot_settings": bot_settings,
                "messages": messages,
                "admins": admins,
                "shortner_settings": shortner_settings
            }
        except Exception as e:
            print(f"Error loading all settings: {e}")
            return {
                "bot_settings": {},
                "messages": {},
                "admins": [],
                "shortner_settings": {}
            }


class LinkShareDB:
    """
    Separate MongoDB connection dedicated to the Link Share Menu.
    Kept fully independent from the file-store MongoDB class above so the
    two features can point at two different MongoDB databases (or even
    two different clusters).
    """
    _instances = {}

    def __new__(cls, uri: str, db_name: str):
        if (uri, db_name) not in cls._instances:
            instance = super().__new__(cls)
            instance.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            instance.db = instance.client[db_name]
            instance.link_share_data = instance.db["link_share_data"]
            cls._instances[(uri, db_name)] = instance
        return cls._instances[(uri, db_name)]

    # ===============================================================
    # LINK SHARE MENU FUNCTIONS

    async def get_link_share_channels(self) -> dict:
        """Return all channels configured for the Link Share Menu."""
        data = await self.link_share_data.find_one({"_id": "link_share_channels"})
        return data.get("channels", {}) if data else {}

    async def add_link_share_channel(self, channel_id: int, channel_data: dict):
        """Add or update a Link Share Menu channel."""
        channels = await self.get_link_share_channels()
        channels[str(channel_id)] = channel_data
        await self.link_share_data.update_one(
            {"_id": "link_share_channels"},
            {"$set": {"channels": channels}},
            upsert=True
        )

    async def remove_link_share_channel(self, channel_id: int) -> bool:
        """Remove a Link Share Menu channel."""
        channels = await self.get_link_share_channels()
        existed = str(channel_id) in channels
        channels.pop(str(channel_id), None)
        await self.link_share_data.update_one(
            {"_id": "link_share_channels"},
            {"$set": {"channels": channels}},
            upsert=True
        )
        return existed

    async def get_link_share_channel(self, channel_id: int):
        channels = await self.get_link_share_channels()
        return channels.get(str(channel_id))

    # Link Share token functions
    async def create_link_share_token(self, token: str, channel_id: int, is_request: bool, expires_at):
        await self.link_share_data.update_one(
            {"_id": f"link_share_token:{token}"},
            {"$set": {
                "token": token,
                "channel_id": channel_id,
                "is_request": is_request,
                "expires_at": expires_at
            }},
            upsert=True
        )

    async def get_link_share_token(self, token: str):
        data = await self.link_share_data.find_one({"_id": f"link_share_token:{token}"})
        if not data:
            return None
        return data

    async def delete_link_share_token(self, token: str):
        await self.link_share_data.delete_one({"_id": f"link_share_token:{token}"})

    # Persistent per-channel Link Share tokens (one stable token per
    # channel per kind, so the Normal/Request Links pages can show a
    # direct, unchanging deep-link button for each channel).
    async def get_link_share_channel_token(self, channel_id: int, kind: str):
        data = await self.link_share_data.find_one({"_id": "link_share_channel_tokens"})
        tokens = data.get("tokens", {}) if data else {}
        return tokens.get(f"{channel_id}:{kind}")

    async def set_link_share_channel_token(self, channel_id: int, kind: str, token: str):
        await self.link_share_data.update_one(
            {"_id": "link_share_channel_tokens"},
            {"$set": {f"tokens.{channel_id}:{kind}": token}},
            upsert=True
        )


# =============================================================================
# Anime Index / Mini App (sync pymongo — separate WEB_DB_URI / WEB_DB_NAME)
# =============================================================================

_client = None
_db = None


class _LazyCol:
    """Proxy so collection attributes resolve after init_db()/_ensure()."""
    def __init__(self, name):
        self._name = name
    def _col(self):
        _ensure()
        return getattr(_db, self._name) if False else _db[self._name]
    def __getattr__(self, item):
        return getattr(self._col(), item)
    def __getitem__(self, item):
        return self._col()[item]


def _ensure():
    global _client, _db
    if _client is not None:
        return
    uri = Config.MONGODB_URL or "mongodb://localhost:27017"
    _client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    _db = _client[Config.MONGODB_NAME]


anime_col = _LazyCol("anime")
users_col = _LazyCol("users")
reports_col = _LazyCol("reports")
requests_col = _LazyCol("requests")
searches_col = _LazyCol("searches")
counters_col = _LazyCol("counters")


def init_db():
    _ensure()
    anime_col.create_index([("source", ASCENDING), ("source_id", ASCENDING)], unique=True)
    anime_col.create_index([("title", ASCENDING)])
    requests_col.create_index([("key", ASCENDING), ("requested_by", ASCENDING)])
    requests_col.create_index([("status", ASCENDING)])
    requests_col.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    requests_col.create_index([("requested_by", ASCENDING), ("seen", ASCENDING)])
    requests_col.create_index([("requested_by", ASCENDING), ("responded_at", ASCENDING)])
    searches_col.create_index([("count", ASCENDING)])


def _next_id(counter_name: str) -> int:
    doc = counters_col.find_one_and_update(
        {"_id": counter_name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc["seq"]


# ---------------------------------------------------------------------------
# Anime catalog
# ---------------------------------------------------------------------------

def _to_anime(doc) -> dict | None:
    if not doc:
        return None
    d = dict(doc)
    d["id"] = d.pop("_id")
    d["genres"] = d.get("genres") or []
    d["available"] = bool(d.get("join_link"))
    return d


def _family_source_ids(source: str, start_related_ids: list[str]) -> set[str]:
    """Walk the AniList franchise relation graph (seasons, OVAs, movies,
    spin-offs, alternates/compilations) across already-posted
    entries, starting from `start_related_ids`, and return every source_id
    reachable — not just the immediate one-hop neighbors. This is what lets
    a link set on Season 1 reach Season 3 even when AniList only records a
    direct edge between 1<->2 and 2<->3, as long as Season 2 is posted."""
    seen: set[str] = set()
    frontier = [str(x) for x in start_related_ids]
    while frontier:
        sid = frontier.pop()
        if sid in seen:
            continue
        seen.add(sid)
        doc = anime_col.find_one({"source": source, "source_id": sid})
        if doc:
            for rel in doc.get("related_ids") or []:
                rel = str(rel)
                if rel not in seen:
                    frontier.append(rel)
    return seen


def find_inherited_link(source: str, related_ids: list[str]) -> str | None:
    """Look for a join link anywhere in the same franchise (walking the
    full relation graph across already-posted entries). Standalone so it
    can be checked *before* a title is saved — e.g. from /addpost, to
    decide whether a brand-new post can be auto-linked immediately instead
    of prompting the admin for a link at all."""
    if not related_ids:
        return None
    family_ids = _family_source_ids(source, related_ids)
    if not family_ids:
        return None
    related_doc = anime_col.find_one({
        "source": source,
        "source_id": {"$in": list(family_ids)},
        "join_link": {"$nin": [None, ""]},
    })
    return related_doc["join_link"] if related_doc else None


def get_franchise_neighbors(details: dict) -> list[dict]:
    """Walk the full franchise relation graph (same traversal as
    _family_source_ids — seasons, OVAs, movies, spin-offs, alternates) and,
    considering only titles that are actually posted (have a join_link),
    line the whole family up in release-chronological order (year, then
    month/day to break ties within a year). Returns just the entry
    immediately before `details` and the entry immediately after it in
    that timeline — never more than two — so a detail sheet always shows
    at most one "Prequel" card and one "Sequel" card, regardless of how
    many AniList relation edges (Side Story, Alternative, Spin-off, ...)
    the title actually has. `details` doesn't need to be posted itself —
    its own year/month/day (from AniList) are used to place it in the
    timeline even before it has a join link, so browsing an unposted
    title still shows correct neighbors once other family members are
    posted."""
    source = details["source"]
    source_id = str(details["source_id"])
    related_ids = [str(x) for x in details.get("related_ids") or []]

    family_ids = _family_source_ids(source, related_ids)
    family_ids.discard(source_id)
    docs = list(anime_col.find({
        "source": source,
        "source_id": {"$in": list(family_ids)},
        "join_link": {"$nin": [None, ""]},
    }))
    docs.append({
        "_id": None,
        "source_id": source_id,
        "title": details.get("title"),
        "poster_url": details.get("poster_url"),
        "year": details.get("year"),
        "start_month": details.get("start_month"),
        "start_day": details.get("start_day"),
    })
    if len(docs) < 2:
        return []

    def sort_key(d):
        return (
            d.get("year") if d.get("year") is not None else 9999,
            d.get("start_month") if d.get("start_month") is not None else 13,
            d.get("start_day") if d.get("start_day") is not None else 32,
            str(d.get("_id")) if d.get("_id") is not None else d["source_id"],
        )

    docs.sort(key=sort_key)
    idx = next(i for i, d in enumerate(docs) if d["source_id"] == source_id)

    out = []
    if idx > 0:
        p = docs[idx - 1]
        out.append({"id": p["_id"], "title": p["title"], "poster_url": p.get("poster_url"), "relation_type": "PREQUEL"})
    if idx < len(docs) - 1:
        s = docs[idx + 1]
        out.append({"id": s["_id"], "title": s["title"], "poster_url": s.get("poster_url"), "relation_type": "SEQUEL"})
    return out


def upsert_anime(details: dict, added_by: int | None = None) -> int:
    """Insert a new catalog entry from a normalized source dict, or update
    the existing one if this (source, source_id) was already posted.

    If this is a brand-new post and any other already-posted title in the
    same franchise (found by walking the full relation graph, not just
    this title's direct AniList relations) already has a join link set,
    the new post automatically inherits that same link — so adding
    "Season 3" of something you've already linked doesn't need a separate
    /editpost, even if Season 2 is the only thing directly linking them.
    """
    now = time.time()
    existing = anime_col.find_one({"source": details["source"], "source_id": str(details["source_id"])})
    related_ids = [str(x) for x in details.get("related_ids", [])]

    fields = {
        "title": details["title"],
        "alt_title": details.get("alt_title"),
        "year": details.get("year"),
        "start_month": details.get("start_month"),
        "start_day": details.get("start_day"),
        "poster_url": details.get("poster_url"),
        "banner_url": details.get("banner_url"),
        "description": details.get("description"),
        "genres": details.get("genres", []),
        "rating": details.get("rating"),
        "status": details.get("status"),
        "episodes": details.get("episodes"),
        "format": details.get("format"),
        "duration": details.get("duration"),
        "related_ids": related_ids,
        "relations": details.get("relations", []),
        "updated_at": now,
    }

    if existing:
        anime_col.update_one({"_id": existing["_id"]}, {"$set": fields})
        return existing["_id"]

    inherited_link = find_inherited_link(details["source"], related_ids)

    new_id = _next_id("anime")
    try:
        anime_col.insert_one({
            "_id": new_id,
            "source": details["source"],
            "source_id": str(details["source_id"]),
            "join_link": inherited_link,
            "added_by": added_by,
            "created_at": now,
            **fields,
        })
        return new_id
    except DuplicateKeyError:
        # A concurrent call for this exact (source, source_id) — e.g. two
        # overlapping franchise-propagation runs both discovering the same
        # unposted related title at the same time, or a client/proxy
        # retrying a slow request — already inserted it between our
        # existence check above and this insert. That other insert wins;
        # just update the doc it created instead of crashing.
        existing = anime_col.find_one({"source": details["source"], "source_id": str(details["source_id"])})
        if not existing:
            raise  # shouldn't happen — surface it rather than hide a real bug
        anime_col.update_one({"_id": existing["_id"]}, {"$set": fields})
        return existing["_id"]


def delete_anime(anime_id: int):
    anime_col.delete_one({"_id": anime_id})


def delete_anime_family(anime_id: int) -> int:
    """Delete anime_id and every other already-posted title in the same
    franchise (seasons, OVAs, movies, spin-offs, etc. — found the same way
    propagate_join_link finds them). Used when a join link is cleared: a
    title with no link isn't a real post anymore, so it (and the rest of
    the family, which loses the same link via propagation) is removed
    from MongoDB entirely rather than left behind as an unlinked,
    unjoinable entry. Returns how many *other* posts (besides anime_id
    itself) were deleted."""
    doc = anime_col.find_one({"_id": anime_id})
    if not doc:
        return 0
    family_ids = _family_source_ids(doc["source"], doc.get("related_ids") or [])
    family_ids.discard(str(doc["source_id"]))
    other_count = 0
    if family_ids:
        result = anime_col.delete_many({"source": doc["source"], "source_id": {"$in": list(family_ids)}})
        other_count = result.deleted_count
    anime_col.delete_one({"_id": anime_id})
    return other_count


def get_anime(anime_id: int) -> dict | None:
    return _to_anime(anime_col.find_one({"_id": anime_id}))


def find_by_source_id(source: str, source_id: str) -> dict | None:
    return _to_anime(anime_col.find_one({"source": source, "source_id": str(source_id)}))


def list_available() -> list[dict]:
    """Every posted title in MongoDB. Since a title is only ever saved
    once it has a join link (see upsert_anime/delete_anime_family), this
    is effectively already "linked only" — but it's still the raw,
    unfiltered query, used directly by admin bot commands (/editpost,
    /delpost, /refreshposts) that need to find a post regardless of
    anything the public-facing API layer additionally filters."""
    docs = anime_col.find().collation({"locale": "en", "strength": 2}).sort("title", ASCENDING)
    return [_to_anime(d) for d in docs]


def search_local(query: str) -> list[dict]:
    docs = (
        anime_col.find({"title": {"$regex": query, "$options": "i"}})
        .collation({"locale": "en", "strength": 2})
        .sort("title", ASCENDING)
    )
    return [_to_anime(d) for d in docs]


def update_link(anime_id: int, link: str):
    anime_col.update_one(
        {"_id": anime_id},
        {"$set": {"join_link": link or None, "updated_at": time.time()}},
    )


def propagate_join_link(anime_id: int, link: str) -> int:
    """After setting (or clearing) anime_id's join link, apply the same
    value to every other already-posted title in the same franchise —
    found by walking the AniList franchise relation graph across posted
    entries, so the whole family (seasons, OVAs, movies, spin-offs, etc.)
    stays in sync either way: a link set anywhere reaches the rest of the
    family, and clearing a link anywhere clears it everywhere too, so a
    removed post also disappears from the "Available" tab across the
    board rather than leaving stale linked entries behind. Returns how
    many other posts were updated."""
    doc = anime_col.find_one({"_id": anime_id})
    if not doc:
        return 0
    family_ids = _family_source_ids(doc["source"], doc.get("related_ids") or [])
    family_ids.discard(str(doc["source_id"]))
    if not family_ids:
        return 0
    result = anime_col.update_many(
        {"source": doc["source"], "source_id": {"$in": list(family_ids)}},
        {"$set": {"join_link": link or None, "updated_at": time.time()}},
    )
    return result.modified_count


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def get_or_create_user(telegram_id: int, username: str | None, first_name: str | None,
                        is_admin: bool) -> dict:
    role = "admin" if is_admin else "member"
    existing = users_col.find_one({"_id": telegram_id})

    if existing:
        users_col.update_one(
            {"_id": telegram_id},
            {"$set": {"username": username, "first_name": first_name, "role": role}},
        )
        existing.update(username=username, first_name=first_name, role=role)
        existing["telegram_id"] = existing.pop("_id")
        return existing

    now = time.time()
    doc = {
        "_id": telegram_id, "username": username, "first_name": first_name,
        "role": role, "access": "active", "registered_at": now,
    }
    users_col.insert_one(dict(doc))
    doc["telegram_id"] = doc.pop("_id")
    return doc


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

def create_report(anime_id: int | None, anime_title: str, reason: str, details: str,
                   reported_by: int | None, reported_by_name: str | None) -> int:
    new_id = _next_id("reports")
    reports_col.insert_one({
        "_id": new_id,
        "anime_id": anime_id,
        "anime_title": anime_title,
        "reason": reason,
        "details": details,
        "reported_by": reported_by,
        "reported_by_name": reported_by_name,
        "created_at": time.time(),
    })
    return new_id


# ---------------------------------------------------------------------------
# Requests — replaces the old Votes "demand signal". Unlike a vote count,
# a request is something an admin actually responds to (accepted/rejected),
# so each request is its own row (not just an incrementing counter) with
# its own status the requester can be notified about. Rows are keyed by a
# normalized (lowercased) title so "One Piece" and "one piece" count as the
# same title, and grouped that way so one admin decision reaches every user
# who asked for it.
# ---------------------------------------------------------------------------

def _request_key(title: str) -> str:
    return title.strip().lower()


def _request_ref(doc: dict) -> str:
    """A short human-facing reference like 'AR-20260727-001' — cosmetic
    (for the notification card footer), built from the request's own id
    and creation date rather than stored separately."""
    date_part = time.strftime("%Y%m%d", time.localtime(doc.get("created_at") or time.time()))
    return f"AR-{date_part}-{str(doc['_id']).zfill(3)}"


DEFAULT_ACCEPT_NOTE = "Good news! Your requested anime has been accepted and will be added soon."
DEFAULT_REJECT_NOTE = "Sorry, we're not able to add this title right now."

REQUEST_PENDING_TTL_SECONDS = 24 * 60 * 60  # an unanswered request auto-deletes after 24h


def _expire_stale_pending_requests() -> None:
    """A request an admin never accepts or rejects would otherwise sit as
    'pending' forever. This process has no always-on background worker to
    run a real scheduled job on — it only runs its event loop while
    actually handling a request (see _delete_message_later's docstring in
    app.py for the same constraint) — so instead this sweep just runs
    opportunistically every time pending requests are created or read,
    which in practice happens often enough that nothing sits expired for
    long."""
    cutoff = time.time() - REQUEST_PENDING_TTL_SECONDS
    requests_col.delete_many({"status": "pending", "created_at": {"$lt": cutoff}})


MAX_PENDING_REQUESTS_PER_USER = 5


def create_request(title: str, source: str | None, source_id, poster_url: str | None,
                    genres: list[str] | None, telegram_id: int, telegram_name: str | None) -> dict:
    """Returns {"status": str, "already_requested": bool, "id": int | None}.
    Each Telegram user gets at most one active request per title — asking
    again while it's still pending (or already accepted) just returns the
    current status. A title that was previously rejected can be requested
    again, which reopens it as a fresh pending request — but only up to
    MAX_PENDING_REQUESTS_PER_USER pending requests at once per user; past
    that, status is "limit_reached" and nothing is written, so someone
    can't flood the admin queue with an unbounded backlog."""
    _expire_stale_pending_requests()
    key = _request_key(title)
    existing = requests_col.find_one({"key": key, "requested_by": telegram_id})

    if existing and existing["status"] != "rejected":
        return {"status": existing["status"], "already_requested": True, "id": existing["_id"]}

    pending_count = requests_col.count_documents({"requested_by": telegram_id, "status": "pending"})
    if pending_count >= MAX_PENDING_REQUESTS_PER_USER:
        return {
            "status": "limit_reached", "already_requested": False, "id": None,
            "limit": MAX_PENDING_REQUESTS_PER_USER,
        }

    now = time.time()
    if existing:
        requests_col.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "status": "pending", "created_at": now, "responded_at": None,
                "seen": True, "note": None, "poster_url": poster_url, "genres": genres or [],
            }},
        )
        return {"status": "pending", "already_requested": False, "id": existing["_id"]}

    new_id = _next_id("requests")
    requests_col.insert_one({
        "_id": new_id,
        "key": key,
        "title": title,
        "source": source,
        "source_id": str(source_id) if source_id is not None else None,
        "poster_url": poster_url,
        "genres": genres or [],
        "requested_by": telegram_id,
        "requested_by_name": telegram_name,
        "status": "pending",
        "created_at": now,
        "responded_at": None,
        "note": None,
        # The requester made this themselves, so there's nothing new for
        # the notification bell to surface yet — "seen" only turns false
        # once an admin changes the status out from under them.
        "seen": True,
    })
    return {"status": "pending", "already_requested": False, "id": new_id}


def list_pending_requests() -> list[dict]:
    """One row per distinct requested title (not per requester), for the
    admin queue — grouped so an admin sees "12 people want Title X" instead
    of 12 separate identical-looking rows."""
    _expire_stale_pending_requests()
    pipeline = [
        {"$match": {"status": "pending"}},
        {"$sort": {"created_at": 1}},
        {"$group": {
            "_id": "$key",
            "title": {"$first": "$title"},
            "source": {"$first": "$source"},
            "source_id": {"$first": "$source_id"},
            "poster_url": {"$first": "$poster_url"},
            "count": {"$sum": 1},
            "first_requested_at": {"$min": "$created_at"},
        }},
        {"$sort": {"count": -1, "first_requested_at": 1}},
    ]
    docs = list(requests_col.aggregate(pipeline))
    for d in docs:
        d["key"] = d.pop("_id")
    return docs


def respond_to_request(key: str, status: str, note: str | None = None) -> int:
    """Apply an admin decision (accepted/rejected) to every pending request
    for this title at once, and flag each as unseen so the requester's
    notification bell picks it up. Returns how many requests were updated."""
    result = requests_col.update_many(
        {"key": key, "status": "pending"},
        {"$set": {
            "status": status,
            "responded_at": time.time(),
            "seen": False,
            "note": note or (DEFAULT_ACCEPT_NOTE if status == "accepted" else DEFAULT_REJECT_NOTE),
        }},
    )
    return result.modified_count


def resolve_request_by_id(request_id: int, status: str, note: str | None = None) -> int | None:
    """Look up a single request row by its own numeric id — this is what
    lets the log-channel Accept/Reject buttons use a short numeric
    callback_data (Telegram caps callback_data at 64 bytes, and a full
    title easily blows past that) — then apply the decision to every
    pending request sharing that title, same as respond_to_request.
    Returns None (instead of a count) if this particular request was
    already resolved, so a double-tap on the log message's buttons is a
    harmless no-op rather than re-firing notifications."""
    doc = requests_col.find_one({"_id": request_id})
    if not doc or doc["status"] != "pending":
        return None
    return respond_to_request(doc["key"], status, note)


def accept_requests_for_title(title: str) -> int:
    """Convenience wrapper around respond_to_request for the common case:
    a title just got posted (a join link was set), so any pending request
    for that exact title is resolved automatically — the admin doesn't
    have to separately visit the requests queue for every title they add
    directly. Returns how many requests were updated."""
    return respond_to_request(_request_key(title), "accepted")


NOTIFICATION_TTL_SECONDS = 24 * 60 * 60  # notifications auto-expire after 24h


def get_user_notifications(telegram_id: int, limit: int = 30) -> dict:
    """The current user's own resolved requests (accepted/rejected) from the
    last 24 hours, most recent first, plus how many of those they haven't
    seen yet — that unseen count is what the notification bell badge shows.
    Anything resolved more than 24h ago has aged out and no longer appears,
    even if it was never opened. A user who has never requested anything
    (or whose requests are all still pending, or all expired) gets an empty
    list and a zero count, i.e. an empty bell."""
    _expire_stale_pending_requests()
    cutoff = time.time() - NOTIFICATION_TTL_SECONDS
    fresh_filter = {
        "requested_by": telegram_id,
        "status": {"$ne": "pending"},
        "responded_at": {"$gte": cutoff},
    }
    docs = requests_col.find(fresh_filter).sort("responded_at", -1).limit(limit)
    notifications = [
        {
            "id": d["_id"],
            "ref": _request_ref(d),
            "title": d["title"],
            "poster_url": d.get("poster_url"),
            "genres": d.get("genres") or [],
            "status": d["status"],
            "note": d.get("note") or (DEFAULT_ACCEPT_NOTE if d["status"] == "accepted" else DEFAULT_REJECT_NOTE),
            "requested_by_name": d.get("requested_by_name"),
            "responded_at": d.get("responded_at"),
            "seen": d.get("seen", True),
        }
        for d in docs
    ]
    unseen_count = requests_col.count_documents({**fresh_filter, "seen": False})
    return {"unseen_count": unseen_count, "notifications": notifications}


def mark_notifications_seen(telegram_id: int) -> None:
    requests_col.update_many(
        {"requested_by": telegram_id, "seen": False},
        {"$set": {"seen": True}},
    )


# ---------------------------------------------------------------------------
# Search tracking — powers the Search page's "Popular Searches" list.
# ---------------------------------------------------------------------------

def record_search(query: str) -> None:
    query = query.strip()
    if len(query) < 2:
        return
    key = query.lower()
    searches_col.update_one(
        {"_id": key},
        {"$setOnInsert": {"display": query}, "$inc": {"count": 1}, "$set": {"last_searched": time.time()}},
        upsert=True,
    )


def get_popular_searches(limit: int = 6) -> list[dict]:
    docs = searches_col.find().sort("count", -1).limit(limit)
    return [{"query": d["display"], "count": d["count"]} for d in docs]


def clear_popular_searches() -> None:
    searches_col.delete_many({})

