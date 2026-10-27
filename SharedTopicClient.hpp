#pragma once

// clang-format off
/* === MODULE MANIFEST V2 ===
module_description: 多 Topic 数据打包与 UART 转发模块 / Module that packs multi-Topic data and forwards it over a UART
depends: []
=== END MANIFEST === */
// clang-format on

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>

#include "libxr_def.hpp"
#include "message.hpp"
#include "serialized_service.hpp"
#include "slot_pool.hpp"
#include "uart.hpp"

/**
 * @brief 订阅多个 Topic，把每次发布打包后通过 UART 转发。
 *        Subscribes to several Topics, packs every publication and forwards it over a
 *        UART.
 */
class SharedTopicClient
{
 private:
  struct CallbackInfo
  {
    SharedTopicClient* client;
    LibXR::Topic::TopicHandle topic;
  };

  struct PacketSlot
  {
    LibXR::RawData buffer;
    size_t packet_size = 0;
  };

 public:
  /**
   * @brief 需要订阅并转发的 Topic。
   *        A Topic to subscribe to and forward.
   */
  struct TopicConfig
  {
    const char* name;                         ///< Topic 名称 Topic name
    const char* domain = "libxr_def_domain";  ///< Topic 所在的 domain Domain of the Topic

    /**
     * @brief 使用默认 domain `libxr_def_domain` 构造。
     *        Construct with the default domain `libxr_def_domain`.
     *
     * @param name Topic 名称。
     *             Topic name.
     */
    TopicConfig(const char* name) : name(name) {}

    /**
     * @brief 构造并指定 domain。
     *        Construct with an explicit domain.
     *
     * @param name Topic 名称。
     *             Topic name.
     * @param domain Topic 所在的 domain。
     *               Domain of the Topic.
     */
    TopicConfig(const char* name, const char* domain) : name(name), domain(domain) {}
  };

  /**
   * @brief 构造 SharedTopicClient：创建槽池，并为 topic_configs 中的每个 Topic
   *        注册 callback。
   *        Construct SharedTopicClient: create the slot pool, and register a
   *        callback on every Topic of topic_configs.
   *
   * @param uart 发送数据包的 UART，须有可写的 write port。
   *             UART the packets are sent on; it must have a writable write port.
   * @param slot_count 共享的待发槽位数量，须大于 0。
   *                   Number of shared pending slots; must be greater than 0.
   * @param topic_configs 需要订阅并转发的 Topic 列表，至少一项；未找到的 Topic
   *                      输出错误日志并进入致命错误处理。
   *                      Topics to subscribe to and forward, at least one; a Topic that
   *                      is not found is logged as an error and enters the fatal error
   *                      handler.
   */
  SharedTopicClient(LibXR::UART& uart, uint32_t slot_count = 16,
                    std::initializer_list<TopicConfig> topic_configs =
                        {"topic1", {"topic2", "libxr_def_domain"}})
      : uart_(std::addressof(uart))
  {
    ASSERT(uart_->write_port_ != nullptr);
    ASSERT(uart_->write_port_->Writable());
    ASSERT(topic_configs.size() > 0);
    ASSERT(slot_count > 0);

    size_t max_packet_size = 0;

    for (auto config : topic_configs)
    {
      auto domain = LibXR::Topic::Domain(config.domain);
      auto topic = LibXR::Topic::Find(config.name, &domain);
      if (topic == nullptr)
      {
        XR_LOG_ERROR("Topic not found: %s/%s", config.domain, config.name);
        // 配置错误：所有构建类型都进入致命错误处理
        // Configuration error: every build type enters the fatal error handler
        libxr_fatal_error(__FILE__, __LINE__, false);
      }
      const size_t packet_size = topic->data_.payload_size + LibXR::Topic::PACK_BASE_SIZE;
      max_packet_size = LibXR::max(max_packet_size, packet_size);
    }

    ASSERT(max_packet_size <= uart_->write_port_->Capacity());

    packets_ = new LibXR::SlotPool<PacketSlot>(slot_count);
    for (uint32_t i = 0; i < slot_count; i++)
    {
      size_t slot_index = 0;
      [[maybe_unused]] const auto result = packets_->Acquire(slot_index);
      ASSERT(result == LibXR::ErrorCode::OK);
      new (std::addressof(packets_->At(slot_index)))
          PacketSlot{LibXR::RawData(new uint8_t[max_packet_size], max_packet_size), 0};
    }
    // 初始化期间持有全部槽位，再统一归还，避免重复初始化同一个空槽。
    // Hold every slot during initialization, then release them all exactly once.
    for (uint32_t i = 0; i < slot_count; i++)
    {
      packets_->Release(i);
    }

    tx_callback_ = LibXR::Callback<LibXR::ErrorCode>::Create(
        [](bool in_isr, SharedTopicClient* self, LibXR::ErrorCode status)
        { self->OnWriteDone(in_isr, status); }, this);
    tx_op_ = LibXR::WriteOperation(tx_callback_);

    for (auto config : topic_configs)
    {
      auto domain = LibXR::Topic::Domain(config.domain);
      auto topic_handle = LibXR::Topic::Find(config.name, &domain);
      // 上一轮循环已确认每个 Topic 都存在
      // The first loop has confirmed that every Topic exists
      ASSERT(topic_handle != nullptr);
      void (*func)(bool, CallbackInfo, const LibXR::Topic::RawMessageView&) =
          [](bool in_isr, CallbackInfo info, const LibXR::Topic::RawMessageView& message)
      { info.client->OnTopic(in_isr, info, message); };

      auto msg_cb =
          LibXR::Topic::Callback::Create(func, CallbackInfo{this, topic_handle});

      LibXR::Topic topic(topic_handle);

      topic.RegisterCallback(msg_cb);
    }
  }

 private:
  void OnTopic(bool in_isr, CallbackInfo info,
               const LibXR::Topic::RawMessageView& message)
  {
    const size_t packet_size = message.payload.size_ + LibXR::Topic::PACK_BASE_SIZE;
    size_t slot_index = 0;

    if (packets_->Acquire(slot_index) != LibXR::ErrorCode::OK)
    {
      return;
    }

    auto& slot = packets_->At(slot_index);
    ASSERT(packet_size <= slot.buffer.size_);
    if (LibXR::Topic(info.topic)
            .PackRaw(message.payload, slot.buffer, message.timestamp) !=
        LibXR::ErrorCode::OK)
    {
      packets_->Release(slot_index);
      return;
    }

    slot.packet_size = packet_size;
    packets_->Publish(slot_index);
    KickTx(in_isr);
  }

  void KickTx(bool in_isr)
  {
    tx_service_.Invoke(1U, in_isr,
                       [this](uint32_t, bool owner_in_isr) { TxService(owner_in_isr); });
  }

  void TxService(bool in_isr)
  {
    // 请求可以合并；每轮处理至多一个槽池容量，重入请求由串行服务继续处理。
    // Requests may coalesce; drain at most one pool capacity per service pass.
    for (size_t count = 0; count < packets_->Size(); ++count)
    {
      size_t slot_index = 0;
      if (packets_->Take(slot_index) != LibXR::ErrorCode::OK)
      {
        return;
      }

      auto& slot = packets_->At(slot_index);
      uart_->Write(LibXR::ConstRawData{slot.buffer.addr_, slot.packet_size}, tx_op_,
                   in_isr);
      // Write 在返回前完成数据复制；拒绝写入时也可立即归还本槽。
      // Write copies admitted data before returning; rejected data can be released too.
      packets_->Release(slot_index);
    }
  }

  void OnWriteDone(bool in_isr, LibXR::ErrorCode)
  {
    // 写完成后请求重新检查待发槽；同步完成时由当前串行服务延后处理。
    // Recheck ready slots after completion; inline callbacks defer to the active service.
    KickTx(in_isr);
  }

  LibXR::UART* uart_;
  LibXR::SlotPool<PacketSlot>* packets_ = nullptr;
  LibXR::SerializedService tx_service_;
  LibXR::Callback<LibXR::ErrorCode> tx_callback_;
  LibXR::WriteOperation tx_op_;
};
