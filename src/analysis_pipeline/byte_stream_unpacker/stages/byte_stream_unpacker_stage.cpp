#include "analysis_pipeline/byte_stream_unpacker/stages/byte_stream_unpacker_stage.h"
#include "analysis_pipeline/unpacker_core/data_products/ByteStream.h"
#include "analysis_pipeline/core/data/pipeline_data_product.h"
#include <spdlog/spdlog.h>

ClassImp(ByteStreamUnpackerStage)

ByteStreamUnpackerStage::ByteStreamUnpackerStage() = default;
ByteStreamUnpackerStage::~ByteStreamUnpackerStage() = default;

void ByteStreamUnpackerStage::OnInit() {
    spdlog::debug("[{}] Initializing ByteStreamUnpackerStage", Name());

    input_product_name_ = parameters_.value("input_product_name", "bytestream_bank_DATA");
    internal_product_name_ = parameters_.value("internal_product_name", "bytestream_bank_DATA");

    spdlog::debug("[{}] Using input_product_name='{}', internal_product_name='{}'",
                  Name(), input_product_name_, internal_product_name_);

    local_config_ = std::make_shared<ConfigManager>();
    if (parameters_.contains("pipeline_config")) {
        if (!local_config_->addJsonObject(parameters_["pipeline_config"])) {
            throw std::runtime_error("Failed to load inline pipeline_config");
        }
    } else if (parameters_.contains("pipeline_config_file")) {
        std::string path = parameters_["pipeline_config_file"].get<std::string>();
        if (!local_config_->loadFiles({path})) {
            throw std::runtime_error("Failed to load pipeline_config_file: " + path);
        }
    } else {
        throw std::runtime_error("ByteStreamUnpackerStage: no pipeline config provided");
    }

    if (!local_config_->validate()) {
        throw std::runtime_error("Internal pipeline configuration failed validation");
    }

    local_pipeline_ = std::make_unique<Pipeline>(local_config_);
    if (!local_pipeline_->buildFromConfig()) {
        throw std::runtime_error("Failed to build internal pipeline");
    }

    spdlog::debug("[{}] Internal pipeline successfully built", Name());
}

void ByteStreamUnpackerStage::Process() {
    if (!getDataProductManager()->hasProduct(input_product_name_)) {
        // Don't need an error, sometimes it's expected behavior that the input product is not available
        // Example: running over events that may or may not includ the requested bank
        spdlog::debug("[{}] Input product '{}' not found", Name(), input_product_name_);
        return;
    }

    auto inputHandle = getDataProductManager()->checkoutRead(input_product_name_);
    if (!inputHandle.get()) {
        spdlog::error("[{}] Failed to lock input product '{}'", Name(), input_product_name_);
        return;
    }

    std::shared_ptr<TObject> base_ptr = inputHandle->getSharedObject();
    if (!base_ptr) {
        spdlog::warn("[{}] Input product has null shared object", Name());
        return;
    }

    auto byte_stream_ptr = std::dynamic_pointer_cast<dataProducts::ByteStream>(base_ptr);
    if (!byte_stream_ptr || !byte_stream_ptr->data || byte_stream_ptr->size == 0) {
        spdlog::warn("[{}] Invalid or empty ByteStream", Name());
        return;
    }

    // Wrap and inject into internal pipeline with internal_product_name_
    auto internal_product = std::make_unique<PipelineDataProduct>();
    internal_product->setName(internal_product_name_);
    internal_product->setSharedObject(byte_stream_ptr);
    internal_product->addTag("byte_stream");
    internal_product->addTag("built_by_byte_stream_unpacker_stage");

    local_pipeline_->getDataProductManager().addOrUpdate(internal_product_name_, std::move(internal_product));

    // Run internal pipeline
    local_pipeline_->execute();

    // Extract all products zero-copy from internal pipeline
    auto& internal_dp_manager = local_pipeline_->getDataProductManager();
    auto product_names = internal_dp_manager.getAllNames();

    if (product_names.empty()) {
        spdlog::warn("[{}] No products found in internal pipeline", Name());
        return;
    }

    std::vector<std::pair<std::string, std::unique_ptr<PipelineDataProduct>>> extracted_products;
    extracted_products.reserve(product_names.size());

    for (const auto& name : product_names) {
        auto extracted = internal_dp_manager.extractProduct(name);
        if (!extracted) {
            spdlog::error("[{}] Failed to extract internal product '{}'", Name(), name);
            continue;
        }
        extracted_products.emplace_back(name, std::move(extracted));
    }

    // Forward extracted products to main pipeline manager (zero copy)
    getDataProductManager()->addOrUpdateMultiple(std::move(extracted_products));

    spdlog::debug("[{}] Finished processing and forwarded internal products", Name());
}




