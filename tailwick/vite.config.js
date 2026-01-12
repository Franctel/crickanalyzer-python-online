import { defineConfig } from 'vite';
import handlebars from 'vite-plugin-handlebars';
import fs from 'fs-extra';
import { resolve } from 'path';

const path = require('path');

const folder = {
    src: "src/", // source files
    dist: "static/", // build files
};

export default defineConfig({
    plugins: [
        handlebars({
            partialDirectory: resolve(__dirname, folder.src),
        }),
    ],
    // logLevel: 'error', // if you want to disable logging use 'info' | 'warn' | 'error' | 'silent'
    clearScreen: true,
    // root: path.resolve(__dirname, folder.src),
    build: {
        // outDir: folder.dist,
        outDir: 'static/',
        emptyOutDir: false,
        // watch: {},  // if you want to watch your build files
        rollupOptions: {
            manualChunks: undefined,
            input: {
                icons: folder.src + 'scss/icons.scss',
                tailwind: folder.src + 'scss/tailwind.scss',
            },
            output: {
                assetFileNames: (css) => {
                    if (css.name.split('.').pop() == 'css') {
                        return 'css/' + `[name]` + '.css';
                    } else if (/png|jpe?g|svg|gif|tiff|bmp|ico/i.test(css.name.split('.').pop())) {
                        return 'images/' + css.name;
                    } else {
                        return 'css/' + css.name;
                    }
                },
                entryFileNames: 'js/' + `[name]` + `.js`,
            },
            external: [
                // Add any other external dependencies here
                /^static\/libs\//, // This regex matches the external import path
            ],
            plugins: [
                // ...other plugins
                require('rollup-plugin-copy')({
                    targets: [
                        { src: folder.src + 'images', dest: folder.dist },
                        { src: folder.src + 'json', dest: folder.dist },
                        { src: folder.src + 'lang', dest: folder.dist },
                        { src: folder.src + 'js', dest: folder.dist },
                        { src: folder.src + 'php', dest: folder.dist },
                    ],
                }),
                { //done yes
                    name: 'copy-specific-packages',
                    async writeBundle() {
                        const outputPath = path.resolve(__dirname, folder.dist); // Adjust the destination path
                        const configPath = path.resolve(__dirname, 'package-libs-config.json');

                        try {
                            const configContent = await fs.readFile(configPath, 'utf-8');
                            const { packagesToCopy } = JSON.parse(configContent);

                            for (const packageName of packagesToCopy) {
                                const destPackagePath = path.join(outputPath, 'libs', packageName);

                                const sourcePath = (fs.existsSync(path.join(__dirname, 'node_modules', packageName + "/dist"))) ?
                                    path.join(__dirname, 'node_modules', packageName + "/dist")
                                    : path.join(__dirname, 'node_modules', packageName);

                                try {
                                    await fs.access(sourcePath, fs.constants.F_OK);
                                    await fs.copy(sourcePath, destPackagePath);
                                } catch (error) {
                                    console.error(`Package ${packageName} does not exist.`);
                                }
                            }
                        } catch (error) {
                            console.error('Error copying and renaming packages:', error);
                        }
                    },
                },
            ],
        },

    },
    publicDir: 'dist',
    server: {
        port: 8080,
        hot: true
    }
})


