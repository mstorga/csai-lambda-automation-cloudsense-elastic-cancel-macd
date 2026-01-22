import kayako_connection


class HelpCenterConnect:
    def __init__(self, is_kayako=True):
        print(f"HelpCenterConnect initializing with is_kayako={is_kayako}")
        self.is_kayako = is_kayako
        self.kayako_agent = None
        
        if self.is_kayako:
            print("Initializing Kayako agent...")
            try:
                self.kayako_agent = kayako_connection.KayakoConnect()
                connection_ok = self.kayako_agent.test_connection()
                if not connection_ok:
                    print("⚠️ WARNING: Kayako connection test failed!")
                else:
                    print("✅ Kayako agent initialized and tested successfully")
            except Exception as e:
                print(f"❌ ERROR: Failed to initialize Kayako agent: {e}")
                self.kayako_agent = None

    def _execute_with_fallback(self, method_name, ticket_id, *args, **kwargs):
        """Execute method with proper error handling"""
        try:
            if self.is_kayako and self.kayako_agent:
                method = getattr(self.kayako_agent, method_name)
                return method(ticket_id, *args, **kwargs)
            else:
                print(f"❌ ERROR: No agent available for {method_name}")
                return None
        except Exception as e:
            print(f"❌ ERROR: {method_name} failed for ticket {ticket_id}: {e}")
            return None

    def delete_tags(self, ticket_id, tags):
        print(f"🏷️ Deleting tags {tags} from ticket {ticket_id}")
        return self._execute_with_fallback('delete_tags', ticket_id, tags)

    def add_tags(self, ticket_id, tags):
        print(f"🏷️ Adding tags {tags} to ticket {ticket_id}")
        return self._execute_with_fallback('add_tags', ticket_id, tags)

    def write_internal_note(self, ticket_id, note):
        print(f"📝 Writing internal note for ticket {ticket_id}")
        return self._execute_with_fallback('write_internal_note', ticket_id, note)
