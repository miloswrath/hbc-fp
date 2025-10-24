{
  description = "A Nix-flake-based Python development environment";

  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1.*.tar.gz";

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSupportedSystem = f: nixpkgs.lib.genAttrs supportedSystems (system: f {
        pkgs = import nixpkgs { inherit system; };
      });

      version = "3.13";
    in {
      devShells = forEachSupportedSystem ({ pkgs }:
        let
          concatMajorMinor = v:
            pkgs.lib.pipe v [
              pkgs.lib.versions.splitVersion
              (pkgs.lib.sublist 0 2)
              pkgs.lib.concatStrings
            ];

          python = pkgs."python${concatMajorMinor version}";


          catppuccin-jupyterlab = python.pkgs.buildPythonPackage rec {
            pname = "catppuccin_jupyterlab";
            version = "0.2.4";
            format = "wheel";

            src = pkgs.fetchPypi {
              inherit pname version;
              format = "wheel";
              python = "py3";
              dist = "py3";
              abi = "none";
              platform = "any";
              # sha256 (SRI) or base32 works:
              hash = "sha256-ZDg5scRuk+SXvrledB1A3VhfxOSJpEwsbOiahpqc72c="; # <-- SRI works if you use 'hash'
            };

            doCheck = false;
          };
          pyts = python.pkgs.buildPythonPackage rec {
            pname = "pyts";
            version = "0.13.0";

            src = pkgs.fetchFromGitHub {
              owner = "johannfaouzi";
              repo = pname;
              rev = "v${version}";
              hash = "sha256-qlETmUO5K+nbSM3dJ2WWuKYwz4dFwqupkoC8BGckdAo=";
            };

            propagatedBuildInputs = with python.pkgs; [
              numpy
              scipy
              scikitlearn
              joblib
              numba
            ];

            doCheck = false; # tests require extra data and heavy deps not in the dev shell
            pythonImportsCheck = [ "pyts" ];
          };
          # One unified Python environment for both Lab (frontend) and the theme
          pythonEnv = python.withPackages (ps:
            [
              ps.jupyterlab
              ps.ipykernel
              ps.pandas
              ps.numpy
              ps.matplotlib
              ps.seaborn
              ps.plotly
              ps.requests
              ps.httpx
              ps.scipy
              ps.pyyaml
              ps.pyreadstat
            ] ++ [ catppuccin-jupyterlab pyts ]  # include the theme and packaged pyts here
          );
        in {
          default = pkgs.mkShellNoCC {
            # IMPORTANT: do NOT activate a venv here; it will shadow pythonEnv
            packages = [
              pythonEnv
              pkgs.git
            ];

            # Optional: install a kernel spec matching this env
            postShellHook = ''
              KERNEL_NAME="jl-313"
              KERNEL_DIR="$HOME/.local/share/jupyter/kernels/$KERNEL_NAME"
              if [ ! -d "$KERNEL_DIR" ]; then
                python -m ipykernel install --user \
                  --name "$KERNEL_NAME" \
                  --display-name "Python 3.13 (flake)" >/dev/null
              fi
            '';
          };
        });
    };
}

