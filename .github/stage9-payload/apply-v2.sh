#!/usr/bin/env bash
set -euo pipefail
printf '%s  %s\n' \
  fb1d7bebfae8b222b7001953668759f269ec5fb4f6a8e859c34a72922beb6e00 .github/stage9-payload/payload-00 \
  5e9551cc01e225aa7c827a1c39c075545420bbe79b8ae5c66453c77d24aacbd2 .github/stage9-payload/payload-01 \
  e0d37dd7161d68b435f8e0d5bebcedb17f05366bab4dc9476c4d510d0c75e54f .github/stage9-payload/payload-02 \
  5c060542eaab185b0dc8e8db59fa772dc23bf5a8a6ed184d587cdb1856c90f69 .github/stage9-payload/payload-03 \
  8de5ddde578d257ffb87ba553ca476bef0dcbea844d8f04f9ec54f308454fb7d .github/stage9-payload/payload-04 \
  c1ab0b877d2928d7e9905a3224ade91b269b52ac04239b8c6c9b21dbf7ad5d93 .github/stage9-payload/payload-05 \
  fef097658cda150729d66be8e6db1f0e48452fe01743502810417195d19f97d8 .github/stage9-payload/payload-06 \
  8b27a2bea5b5c224d3fe45ff27e893bc3e334d0603c4373418ee95850f61a09f .github/stage9-payload/payload-07 | sha256sum -c -
cat .github/stage9-payload/payload-[0-9][0-9] > /tmp/stage9.patch.xz.b64
echo '30e976f7e0ac3f62fdc215804915aab7e12e5ee27912d132496cf8c6872918f9  /tmp/stage9.patch.xz.b64' | sha256sum -c -
base64 -d /tmp/stage9.patch.xz.b64 > /tmp/stage9.patch.xz
echo 'f032e49e0e2a7d7870ff3aca1c9ae64bd641322f9fcc900324b18a42b4aeac2f  /tmp/stage9.patch.xz' | sha256sum -c -
xz -dc /tmp/stage9.patch.xz > /tmp/stage9.patch
echo 'ffa652ac4ceaa623d7c0288be452bd8962090d0d54e99b43f058f8c92e639204  /tmp/stage9.patch' | sha256sum -c -
git apply --check --whitespace=error-all /tmp/stage9.patch
git apply /tmp/stage9.patch
echo '3fdf48845339f5c802563b018852bfe6a6653677cddd6aa1265137b6f641768c  .github/stage9-payload/fix.patch' | sha256sum -c -
git apply --check --whitespace=error-all .github/stage9-payload/fix.patch
git apply .github/stage9-payload/fix.patch
echo '504671bc72ff414c250954e22bbcafe7442aed9b446b75da77568c3b2e30a5ce  .github/stage9-payload/fix2.patch' | sha256sum -c -
git apply --check --whitespace=error-all .github/stage9-payload/fix2.patch
git apply .github/stage9-payload/fix2.patch
echo '216bd68a5983057eaec26cd4c2dfc0622d5386c4af58e919e920b656ddde2c15  .github/stage9-payload/fix3.patch' | sha256sum -c -
git apply --check --whitespace=error-all .github/stage9-payload/fix3.patch
git apply .github/stage9-payload/fix3.patch
echo '25a6996acb1cf1fab3f4ba11e28a3b2e2a2cbc795baf7d4fe420425436a16340  .github/stage9-payload/fix4.patch' | sha256sum -c -
git apply --check --whitespace=error-all .github/stage9-payload/fix4.patch
git apply .github/stage9-payload/fix4.patch
git diff --check
