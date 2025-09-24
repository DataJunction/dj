"""DataJunction Chat Assistant Module"""

import asyncio
import json
import logging
import os

import yaml
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from openai import OpenAI

logger = logging.getLogger(__name__)


class DataJunctionChat:
    """DataJunction Chat Assistant with MCP and LLM fallback support."""

    def __init__(self):
        self.mcp_config = None

    async def start_chat_session(self):
        """
        Start a conversational chat session with DataJunction MCP server.
        """
        config_path = "mcp-server.yaml"
        
        print("🤖 DataJunction Chat Assistant")
        print("=" * 50)
        
        # Load MCP server configuration
        mcp_config = self.load_mcp_config(config_path)
        if not mcp_config:
            print("❌ No MCP server configuration found.")
            print(f"Please create '{config_path}' with your DataJunction MCP server details.")
            return
        
        server_config = mcp_config.get('server', {})
        command = server_config.get('command')
        if not command:
            print("❌ No server command specified in configuration.")
            return
        
        args = server_config.get('args', [])
        env = server_config.get('env', {})
        description = server_config.get('description', 'DataJunction MCP Server')
        
        print(f"🔗 Connecting to: {description}")
        print(f"📋 Command: {command} {' '.join(args)}")
        
        # Set environment variables
        old_env = {}
        for key, value in env.items():
            if value:  # Only set non-empty values
                old_env[key] = os.environ.get(key)
                os.environ[key] = str(value)
        
        try:
            # Connect to MCP server using standard MCP client
            print("🔄 Starting MCP server...")
            
            server_params = StdioServerParameters(
                command=command,
                args=args,
                env=dict(os.environ) if env else None
            )
            
            async with stdio_client(server_params) as streams:
                read_stream, write_stream = streams
                
                client_session = ClientSession(read_stream, write_stream)
                
                try:
                    await asyncio.wait_for(client_session.initialize(), timeout=15.0)
                    print("✅ Connected successfully!")
                except asyncio.TimeoutError:
                    print("⚠️  Server initialization timed out, but continuing...")
                except Exception as e:
                    print(f"⚠️  Initialization warning: {e}, but continuing...")
                
                print("💬 You can now chat with DataJunction. Type 'quit' to exit.")
                print("-" * 50)
                
                # Get available tools
                try:
                    tools_result = await client_session.list_tools()
                    if tools_result.tools:
                        tool_names = [tool.name for tool in tools_result.tools]
                        print(f"🛠️  Available tools: {', '.join(tool_names)}")
                    else:
                        print("ℹ️  No tools available yet (server may still be initializing)")
                except Exception as e:
                    print(f"⚠️  Could not list tools: {e}")
                
                print("-" * 50)
                
                # Start conversational loop
                await self.mcp_conversation_loop(client_session)
            
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            print(f"❌ MCP Connection failed: {e}")
            print("\n🤖 Falling back to basic LLM chat mode...")
            print("💡 I can still help with DataJunction questions using my general knowledge!")
            print("-" * 50)
            
            # Start LLM-based chat as fallback
            await self.llm_chat_mode()
        finally:
            # Restore environment variables
            for key, old_value in old_env.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value
            
            print("\n👋 Chat session ended.")

    async def mcp_conversation_loop(self, client_session: ClientSession):
        """
        Handle the conversational chat loop for MCP mode.
        """
        conversation_history = []
        
        while True:
            try:
                user_input = input("\n💬 You: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['quit', 'exit', 'bye', 'q']:
                    break
                
                if user_input.lower() in ['help', '?']:
                    self.show_mcp_chat_help()
                    continue
                
                # Add user message to conversation history
                conversation_history.append({"role": "user", "content": user_input})
                
                print("🤖 Assistant: ", end="", flush=True)
                
                # Process the user's message
                response = await self.process_user_message(client_session, user_input, conversation_history)
                
                print(response)
                
                # Add assistant response to conversation history
                conversation_history.append({"role": "assistant", "content": response})
                
                # Keep conversation history manageable (last 10 exchanges)
                if len(conversation_history) > 20:
                    conversation_history = conversation_history[-20:]
                
            except KeyboardInterrupt:
                print("\n\n💡 Tip: Type 'quit' to exit gracefully.")
                continue
            except EOFError:
                break
            except Exception as e:
                logger.error(f"Error in conversation loop: {e}")
                print(f"\n❌ An error occurred: {e}")

    async def process_user_message(self, client_session: ClientSession, message: str, history: list) -> str:
        """
        Process user message and generate appropriate response using MCP tools.
        """
        try:
            # Get available tools
            tools_result = await client_session.list_tools()
            
            if not tools_result.tools:
                return "I'm sorry, but no DataJunction tools are available right now. The server may still be starting up."
            
            # Convert tools to dict format for compatibility
            tools = []
            for tool in tools_result.tools:
                tools.append({
                    'name': tool.name,
                    'description': tool.description,
                    'schema': tool.inputSchema if hasattr(tool, 'inputSchema') else None
                })
            
            # Simple intent detection based on keywords
            intent = self.detect_intent(message)
            
            # Find appropriate tool based on intent
            selected_tool = self.select_tool(intent, message, tools)
            
            if selected_tool:
                # Extract parameters for the tool
                tool_args = self.extract_tool_parameters(selected_tool, message)
                
                # Call the MCP tool
                try:
                    result = await client_session.call_tool(selected_tool['name'], tool_args)
                    
                    # Format the response in a conversational way
                    return self.format_tool_response(selected_tool, result.content, message)
                    
                except Exception as e:
                    return f"I encountered an error while trying to help: {e}"
            else:
                # No specific tool found, provide general help
                tool_names = [tool['name'] for tool in tools]
                return (f"I'm not sure how to help with that specific request. "
                       f"I can help you with: {', '.join(tool_names)}. "
                       f"Could you rephrase your question or ask about something more specific?")
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return f"I'm having trouble processing your request right now: {e}"

    def detect_intent(self, message: str) -> str:
        """
        Simple intent detection based on keywords.
        """
        message_lower = message.lower()
        
        if any(word in message_lower for word in ['list', 'show', 'display', 'what are', 'see']):
            return 'list'
        elif any(word in message_lower for word in ['create', 'add', 'new', 'make']):
            return 'create'
        elif any(word in message_lower for word in ['update', 'modify', 'change', 'edit']):
            return 'update'
        elif any(word in message_lower for word in ['delete', 'remove', 'drop']):
            return 'delete'
        elif any(word in message_lower for word in ['query', 'select', 'find', 'search', 'get']):
            return 'query'
        elif any(word in message_lower for word in ['help', 'how', 'what can']):
            return 'help'
        else:
            return 'general'

    def select_tool(self, intent: str, message: str, tools: list) -> dict:
        """
        Select the most appropriate tool based on intent and message content.
        """
        # Score each tool based on relevance
        best_tool = None
        best_score = 0
        
        for tool in tools:
            tool_name = tool.get('name', '').lower()
            tool_desc = tool.get('description', '').lower()
            
            score = 0
            
            # Intent-based scoring
            if intent == 'list' and any(word in tool_name for word in ['list', 'get', 'show']):
                score += 10
            elif intent == 'create' and any(word in tool_name for word in ['create', 'add']):
                score += 10
            elif intent == 'query' and any(word in tool_name for word in ['query', 'search', 'find']):
                score += 10
            elif intent == 'update' and any(word in tool_name for word in ['update', 'modify']):
                score += 10
            elif intent == 'delete' and any(word in tool_name for word in ['delete', 'remove']):
                score += 10
            
            # Keyword matching in message
            message_words = message.lower().split()
            tool_words = (tool_name + ' ' + tool_desc).split()
            
            common_words = set(message_words) & set(tool_words)
            score += len(common_words)
            
            if score > best_score:
                best_score = score
                best_tool = tool
        
        return best_tool if best_score > 0 else None

    def extract_tool_parameters(self, tool: dict, message: str) -> dict:
        """
        Extract parameters for the selected tool from the user message.
        """
        # This is a simple implementation - in a real system, you'd want more sophisticated NLP
        params = {}
        
        # Common parameter patterns
        words = message.split()
        
        # Look for quoted strings (names, descriptions, etc.)
        import re
        quoted_strings = re.findall(r'"([^"]*)"', message)
        if quoted_strings:
            params['name'] = quoted_strings[0]
        
        # Look for common DataJunction entities
        if 'node' in message.lower():
            # Extract node name if mentioned
            node_matches = re.findall(r'node\s+(\w+)', message.lower())
            if node_matches:
                params['node_name'] = node_matches[0]
        
        if 'namespace' in message.lower():
            # Extract namespace if mentioned  
            namespace_matches = re.findall(r'namespace\s+(\w+)', message.lower())
            if namespace_matches:
                params['namespace'] = namespace_matches[0]
        
        return params

    def format_tool_response(self, tool: dict, content_list: list, original_message: str) -> str:
        """
        Format the tool response in a conversational way.
        """
        tool_name = tool.get('name', 'unknown tool')
        
        try:
            # Handle MCP content format (list of content items)
            if not content_list:
                return "The operation completed successfully, but there's no additional information to show."
            
            responses = []
            for content in content_list:
                if hasattr(content, 'type') and content.type == "text":
                    responses.append(content.text)
                elif hasattr(content, 'text'):
                    responses.append(content.text)
                elif isinstance(content, str):
                    responses.append(content)
                else:
                    responses.append(str(content))
            
            combined_response = "\n".join(responses)
            
            if combined_response.strip():
                return combined_response
            else:
                return f"I used {tool_name} successfully, but got no readable output."
        
        except Exception as e:
            return f"I completed the task using {tool_name}, but had trouble formatting the response: {e}"

    def show_mcp_chat_help(self):
        """
        Show help information for the MCP chat interface.
        """
        print("\n🆘 DataJunction MCP Chat Help")
        print("-" * 30)
        print("💬 You can ask me about DataJunction in natural language!")
        print("\nExamples:")
        print("  • 'List all nodes'")
        print("  • 'Show me the databases'") 
        print("  • 'Create a new node called orders'")
        print("  • 'What tables are in the sales namespace?'")
        print("  • 'Help me understand the schema'")
        print("\nCommands:")
        print("  • 'help' or '?' - Show this help")
        print("  • 'quit' or 'exit' - End the chat session")

    async def llm_chat_mode(self):
        """
        Fallback LLM-based chat mode when MCP server is unavailable.
        """
        # Check for OpenAI API key
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            print("⚠️  No OPENAI_API_KEY found in environment variables.")
            print("To use LLM chat mode, please set your OpenAI API key:")
            print("export OPENAI_API_KEY='your-api-key-here'")
            print("\nFalling back to basic help mode...")
            await self.basic_help_mode()
            return

        try:
            client = OpenAI(api_key=api_key)
            print("✅ LLM chat mode ready!")
            print("💬 Ask me anything about DataJunction. Type 'quit' to exit.")
            print("-" * 50)
            
            conversation_history = [
                {
                    "role": "system", 
                    "content": self.get_datajunction_system_prompt()
                }
            ]
            
            while True:
                try:
                    user_input = input("\n💬 You: ").strip()
                    
                    if not user_input:
                        continue
                    
                    if user_input.lower() in ['quit', 'exit', 'bye', 'q']:
                        break
                    
                    if user_input.lower() in ['help', '?']:
                        self.show_llm_chat_help()
                        continue
                    
                    # Add user message to conversation
                    conversation_history.append({"role": "user", "content": user_input})
                    
                    print("🤖 Assistant: ", end="", flush=True)
                    
                    # Get LLM response
                    try:
                        response = client.chat.completions.create(
                            model="gpt-3.5-turbo",
                            messages=conversation_history,
                            max_tokens=500,
                            temperature=0.7,
                            stream=True
                        )
                        
                        assistant_response = ""
                        for chunk in response:
                            if chunk.choices[0].delta.content:
                                content = chunk.choices[0].delta.content
                                print(content, end="", flush=True)
                                assistant_response += content
                        
                        print()  # New line after streaming
                        
                        # Add assistant response to conversation
                        conversation_history.append({"role": "assistant", "content": assistant_response})
                        
                        # Keep conversation manageable
                        if len(conversation_history) > 20:
                            conversation_history = [conversation_history[0]] + conversation_history[-19:]
                    
                    except Exception as e:
                        print(f"\n❌ Error getting LLM response: {e}")
                        print("Please check your OpenAI API key and try again.")
                
                except KeyboardInterrupt:
                    print("\n\n💡 Tip: Type 'quit' to exit gracefully.")
                    continue
                except EOFError:
                    break
        
        except Exception as e:
            print(f"❌ Error setting up LLM chat: {e}")
            print("Falling back to basic help mode...")
            await self.basic_help_mode()

    async def basic_help_mode(self):
        """
        Basic help mode when neither MCP nor LLM is available.
        """
        print("🆘 Basic DataJunction Help")
        print("=" * 40)
        print("I'm running in basic mode. Here's what I know about DataJunction:")
        print()
        self.show_datajunction_info()
        
        while True:
            try:
                user_input = input("\n💬 You (type 'quit' to exit): ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['quit', 'exit', 'bye', 'q']:
                    break
                
                # Provide basic keyword-based responses
                response = self.get_basic_response(user_input)
                print(f"\n🤖 Assistant: {response}")
            
            except (KeyboardInterrupt, EOFError):
                break

    def get_datajunction_system_prompt(self) -> str:
        """
        Get the system prompt for DataJunction-aware LLM responses.
        """
        return """You are a helpful DataJunction assistant. DataJunction is a metrics platform that helps organizations build and manage a universal semantic layer for their data.

Key DataJunction concepts:
- **Nodes**: The core building blocks representing tables, metrics, dimensions, and transformations
- **Dimensions**: Attributes that can be used for grouping and filtering 
- **Metrics**: Quantitative measurements that can be aggregated
- **Namespaces**: Organizational units for grouping related nodes
- **Catalogs**: Data source connections and metadata management
- **Query Engine**: Executes queries across different data sources
- **Semantic Layer**: Provides a unified view of metrics and dimensions

You should:
1. Help users understand DataJunction concepts and architecture
2. Provide guidance on building metrics and dimensions
3. Explain best practices for data modeling
4. Help troubleshoot common issues
5. Suggest appropriate DataJunction patterns for their use cases

Be helpful, concise, and technically accurate. When you don't know something specific about the current DataJunction instance, be honest and suggest they check the documentation or contact their DataJunction admin.
"""

    def show_llm_chat_help(self):
        """
        Show help for LLM chat mode.
        """
        print("\n🆘 DataJunction LLM Chat Help")
        print("-" * 35)
        print("💬 I'm powered by an LLM and can help with DataJunction questions!")
        print("\nI can help you with:")
        print("  • Understanding DataJunction concepts (nodes, metrics, dimensions)")
        print("  • Best practices for data modeling")
        print("  • Architectural guidance")
        print("  • Troubleshooting common issues")
        print("  • Query patterns and optimization")
        print("\nCommands:")
        print("  • 'help' or '?' - Show this help")
        print("  • 'quit' or 'exit' - End the chat session")

    def show_datajunction_info(self):
        """
        Show basic DataJunction information.
        """
        print("📊 **DataJunction Overview:**")
        print("   A metrics platform for building universal semantic layers")
        print()
        print("🔧 **Core Concepts:**")
        print("   • Nodes - Building blocks (tables, metrics, dimensions)")
        print("   • Metrics - Quantitative measurements") 
        print("   • Dimensions - Attributes for grouping/filtering")
        print("   • Namespaces - Organizational units")
        print("   • Catalogs - Data source connections")
        print()
        print("📚 **Common Commands:**")
        print("   • dj deploy <directory> - Deploy node definitions")
        print("   • dj pull <namespace> <directory> - Export nodes to YAML")
        print("   • dj seed - Initialize system metadata")

    def get_basic_response(self, user_input: str) -> str:
        """
        Provide basic keyword-based responses when LLM is unavailable.
        """
        user_input_lower = user_input.lower()
        
        if any(word in user_input_lower for word in ['node', 'nodes']):
            return "Nodes are the core building blocks in DataJunction. They represent tables, metrics, dimensions, and transformations. You can deploy nodes using 'dj deploy' command."
        
        elif any(word in user_input_lower for word in ['metric', 'metrics']):
            return "Metrics in DataJunction are quantitative measurements that can be aggregated. They're defined as nodes with specific computation logic and can be used across different dimensions."
        
        elif any(word in user_input_lower for word in ['dimension', 'dimensions']):
            return "Dimensions are attributes used for grouping and filtering data. They help slice metrics in different ways and are essential for building flexible analytics."
        
        elif any(word in user_input_lower for word in ['namespace', 'namespaces']):
            return "Namespaces organize related nodes together. Use 'dj pull <namespace> <directory>' to export all nodes from a namespace."
        
        elif any(word in user_input_lower for word in ['deploy', 'deployment']):
            return "Use 'dj deploy <directory>' to deploy node YAML definitions to your DataJunction instance. Add --dryrun flag to validate without deploying."
        
        elif any(word in user_input_lower for word in ['query', 'queries', 'sql']):
            return "DataJunction generates optimized SQL queries based on your metric and dimension requests. The query engine handles cross-database joins and aggregations."
        
        elif any(word in user_input_lower for word in ['catalog', 'catalogs']):
            return "Catalogs in DataJunction manage data source connections and metadata. They define how to connect to your databases and data warehouses."
        
        elif any(word in user_input_lower for word in ['help', 'command', 'commands']):
            return "Main DataJunction CLI commands: 'dj deploy', 'dj pull', 'dj seed', 'dj chat'. Use --help with any command for details."
        
        else:
            return ("I can help with DataJunction concepts like nodes, metrics, dimensions, namespaces, and deployment. "
                   "Try asking about specific topics or use the CLI commands 'dj deploy', 'dj pull', or 'dj seed'.")

    def load_mcp_config(self, config_path: str) -> dict:
        """
        Load MCP server configuration from YAML file.
        """
        if not os.path.exists(config_path):
            return None
            
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading MCP config: {e}")
            return None