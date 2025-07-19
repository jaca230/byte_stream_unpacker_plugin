#ifndef ANALYSIS_PIPELINE_STAGES_BYTESTREAM_UNPACKER_STAGE_H
#define ANALYSIS_PIPELINE_STAGES_BYTESTREAM_UNPACKER_STAGE_H

#include "analysis_pipeline/core/stages/base_stage.h"
#include "analysis_pipeline/config/config_manager.h"
#include "analysis_pipeline/pipeline/pipeline.h"

class ByteStreamUnpackerStage : public BaseStage {
public:
    ByteStreamUnpackerStage();
    ~ByteStreamUnpackerStage() override;

    void Process() override;
    std::string Name() const override { return "ByteStreamUnpackerStage"; }

protected:
    void OnInit() override;

private:
    std::string input_product_name_;    // external product name to read from
    std::string internal_product_name_; // the name the external product will be registered as in the internal pipeline
    std::shared_ptr<ConfigManager> local_config_;
    std::unique_ptr<Pipeline> local_pipeline_;

    ClassDefOverride(ByteStreamUnpackerStage, 1);
};

#endif // ANALYSIS_PIPELINE_STAGES_BYTESTREAM_UNPACKER_STAGE_H
